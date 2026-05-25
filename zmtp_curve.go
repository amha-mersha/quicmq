package quicmq

// zmtp_curve.go implements the ZMTP 3.1 CURVE security mechanism.
//
// Protocol references:
//   https://rfc.zeromq.org/spec:25/ZMTP-CURVE/  — ZMTP-CURVE command mapping
//   https://rfc.zeromq.org/spec:26/CURVEZMQ/    — CurveZMQ crypto protocol
//
// Cryptography: Curve25519 ECDH key exchange + XSalsa20-Poly1305 AEAD
// via golang.org/x/crypto/nacl/box (asymmetric) and nacl/secretbox (symmetric).
//
// Handshake (client → server perspective):
//
//   1. Both sides exchange 64-byte ZMTP greetings with mechanism = "CURVE".
//   2. CLIENT → SERVER: HELLO     (ephemeral pubkey + proof-of-liveness box)
//   3. SERVER → CLIENT: WELCOME   (server ephemeral pubkey + cookie, encrypted)
//   4. CLIENT → SERVER: INITIATE  (client permanent pubkey + vouch + metadata,
//                                  encrypted with ephemeral session key)
//   5. SERVER → CLIENT: READY     (socket-type metadata, encrypted)
//   6. All subsequent ZMQ frames are exchanged as encrypted MESSAGE commands.

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync/atomic"

	"golang.org/x/crypto/nacl/box"
	"golang.org/x/crypto/nacl/secretbox"
)

// CurveKey holds a Curve25519 key pair for the ZMTP CURVE mechanism.
type CurveKey struct {
	Public [32]byte
	Secret [32]byte
}

// GenerateCurveKey generates a fresh Curve25519 key pair using crypto/rand.
func GenerateCurveKey() (CurveKey, error) {
	pub, sec, err := box.GenerateKey(rand.Reader)
	if err != nil {
		return CurveKey{}, fmt.Errorf("quicmq: generate curve key: %w", err)
	}
	return CurveKey{Public: *pub, Secret: *sec}, nil
}

// curveTCPMarker is implemented by net.Conn values that carry CURVE key material.
// Open() checks for this interface before the plain zmtpMarker check so that
// CURVE takes priority over the NULL mechanism.
type curveTCPMarker interface {
	curveServerKey() CurveKey        // server permanent keypair (server side)
	curveClientKey() (CurveKey, [32]byte) // (client keypair, server permanent public key)
}

// curveSession holds encryption state established during the CURVE handshake.
// The same precomputed NaCl shared key is used for both directions; the nonce
// prefix ("CurveZMQCLIENT--" vs "CurveZMQSERVER--") keeps send and receive
// streams independent.
type curveSession struct {
	isServer  bool
	sharedKey [32]byte      // precomputed via box.Precompute
	sendCtr   atomic.Uint64 // monotonically increasing send nonce counter
}

func newCurveSession(isServer bool, mySK, peerPK [32]byte) *curveSession {
	cs := &curveSession{isServer: isServer}
	box.Precompute(&cs.sharedKey, &peerPK, &mySK)
	return cs
}

// Nonce prefixes (16-char for 8-byte counter, 8-char for 16-byte random suffix).
const (
	cNonceHello    = "CurveZMQHELLO---" // HELLO box
	cNonceInitiate = "CurveZMQINITIATE" // INITIATE box
	cNonceClient   = "CurveZMQCLIENT--" // client→server data
	cNonceServer   = "CurveZMQSERVER--" // server→client data
	cNonceVouch    = "VOUCH---"          // vouch box inside INITIATE
	cNonceWelcome  = "WELCOME-"          // WELCOME box
	cNonceCookie   = "COOKIE--"          // symmetric cookie encryption
)

// nonce24 builds a 24-byte NaCl nonce by concatenating prefix and suffix.
func nonce24(prefix string, suffix []byte) [24]byte {
	var n [24]byte
	copy(n[:], prefix)
	copy(n[len(prefix):], suffix)
	return n
}

// nonce24u64 builds a 24-byte nonce with a big-endian uint64 counter suffix.
func nonce24u64(prefix string, counter uint64) [24]byte {
	var suf [8]byte
	binary.BigEndian.PutUint64(suf[:], counter)
	return nonce24(prefix, suf[:])
}

func randN(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	return b, err
}

// ─── Greeting ──────────────────────────────────────────────────────────────────

// zmtpBuildCURVEGreeting writes a 64-byte ZMTP greeting with mechanism "CURVE".
func zmtpBuildCURVEGreeting(dst []byte, asServer bool) {
	zmtpBuildGreeting(dst, asServer) // fills signature, version, as-server flag
	// Override mechanism field (bytes 12-31) with "CURVE" + zero padding.
	for i := 12; i < 32; i++ {
		dst[i] = 0
	}
	copy(dst[12:], "CURVE")
}

// zmtpCURVEExchangeGreetings sends and receives 64-byte CURVE greetings.
func zmtpCURVEExchangeGreetings(rw net.Conn, server bool) error {
	greeting := make([]byte, zmtpGreetingSize)
	zmtpBuildCURVEGreeting(greeting, server)
	if _, err := rw.Write(greeting); err != nil {
		return fmt.Errorf("zmtp curve: send greeting: %w", err)
	}
	var peer [zmtpGreetingSize]byte
	if _, err := io.ReadFull(rw, peer[:]); err != nil {
		return fmt.Errorf("zmtp curve: read greeting: %w", err)
	}
	if peer[0] != 0xFF || peer[9] != 0x7F {
		return fmt.Errorf("zmtp curve: invalid peer greeting signature")
	}
	if peer[10] != 3 {
		return fmt.Errorf("zmtp curve: unsupported peer version %d", peer[10])
	}
	// Bytes 12-16 hold the 5-char mechanism name.
	if string(peer[12:17]) != "CURVE" {
		return fmt.Errorf("zmtp curve: peer mechanism %q is not CURVE", string(peer[12:17]))
	}
	return nil
}

// ─── Handshake dispatcher ──────────────────────────────────────────────────────

// zmtpCURVEHandshake performs the full CURVE handshake (greeting + command
// exchange) and returns a curveSession for subsequent message encryption.
func zmtpCURVEHandshake(rw net.Conn, sockType SocketType, isServer bool, ctm curveTCPMarker) (*curveSession, error) {
	if err := zmtpCURVEExchangeGreetings(rw, isServer); err != nil {
		return nil, err
	}
	if isServer {
		return zmtpCURVEServer(rw, sockType, ctm.curveServerKey())
	}
	clientKey, serverPK := ctm.curveClientKey()
	return zmtpCURVEClient(rw, sockType, clientKey, serverPK)
}

// ─── Client handshake ──────────────────────────────────────────────────────────

func zmtpCURVEClient(rw net.Conn, sockType SocketType, clientKey CurveKey, serverPublicKey [32]byte) (*curveSession, error) {
	// Generate client ephemeral (transient) keypair C'/c'.
	ctPub, ctSec, err := box.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("zmtp curve client: generate transient key: %w", err)
	}

	// ── 1. HELLO ────────────────────────────────────────────────────────────────
	// Box[64 zeros](c'→S, nonce=cNonceHello+1)
	helloNonce := nonce24u64(cNonceHello, 1)
	helloBox := box.Seal(nil, make([]byte, 64), &helloNonce, &serverPublicKey, ctSec)

	hello := make([]byte, 0, 6+2+70+32+80)
	hello = append(hello, '\x05', 'H', 'E', 'L', 'L', 'O') // name (length-prefixed)
	hello = append(hello, 1, 0)                              // version 1.0
	hello = append(hello, make([]byte, 70)...)               // padding
	hello = append(hello, ctPub[:]...)                       // 32-byte client transient pubkey
	hello = append(hello, helloBox...)                       // 80-byte box
	if err := zmtpWriteFrame(rw, 0x04, hello); err != nil {
		return nil, fmt.Errorf("zmtp curve client: send HELLO: %w", err)
	}

	// ── 2. WELCOME ──────────────────────────────────────────────────────────────
	// Body: "\x07WELCOME"(8) + nonce-suffix(16) + box(144)
	_, welcomeBody, err := zmtpReadRawFrame(rw)
	if err != nil {
		return nil, fmt.Errorf("zmtp curve client: read WELCOME: %w", err)
	}
	if len(welcomeBody) < 8+16+144 {
		return nil, fmt.Errorf("zmtp curve client: WELCOME too short (%d bytes)", len(welcomeBody))
	}
	if string(welcomeBody[:8]) != "\x07WELCOME" {
		return nil, fmt.Errorf("zmtp curve client: expected WELCOME command")
	}
	welcomeNonce := nonce24(cNonceWelcome, welcomeBody[8:24])
	// Sealed with server permanent key → client transient pubkey.
	welcomeDecrypted, ok := box.Open(nil, welcomeBody[24:], &welcomeNonce, &serverPublicKey, ctSec)
	if !ok {
		return nil, fmt.Errorf("zmtp curve client: decrypt WELCOME failed")
	}
	if len(welcomeDecrypted) < 32+96 {
		return nil, fmt.Errorf("zmtp curve client: WELCOME plaintext too short")
	}
	var stPub [32]byte
	copy(stPub[:], welcomeDecrypted[:32])
	cookie := welcomeDecrypted[32:128] // 96 bytes — echoed back in INITIATE

	// ── 3. INITIATE ─────────────────────────────────────────────────────────────
	// Vouch = Box[ctPub](c→S', "VOUCH---"+16-byte-random)
	// Proves client permanent key controls the transient key.
	vouchNonceSuf, err := randN(16)
	if err != nil {
		return nil, fmt.Errorf("zmtp curve client: random vouch nonce: %w", err)
	}
	vouchNonce := nonce24(cNonceVouch, vouchNonceSuf)
	vouchBox := box.Seal(nil, ctPub[:], &vouchNonce, &stPub, &clientKey.Secret)
	// vouch = 16 (nonce suffix) + 48 (32+16 overhead) = 64 bytes

	metadata := zmtpEncodeProperty("Socket-Type", string(sockType))
	initPlain := make([]byte, 0, 32+64+len(metadata))
	initPlain = append(initPlain, clientKey.Public[:]...) // 32
	initPlain = append(initPlain, vouchNonceSuf...)        // 16
	initPlain = append(initPlain, vouchBox...)             // 48
	initPlain = append(initPlain, metadata...)

	initNonce := nonce24u64(cNonceInitiate, 1)
	initBox := box.Seal(nil, initPlain, &initNonce, &stPub, ctSec)

	var initNonceSuf [8]byte
	binary.BigEndian.PutUint64(initNonceSuf[:], 1)

	initiate := make([]byte, 0, 9+96+8+len(initBox))
	initiate = append(initiate, '\x08', 'I', 'N', 'I', 'T', 'I', 'A', 'T', 'E')
	initiate = append(initiate, cookie...)
	initiate = append(initiate, initNonceSuf[:]...)
	initiate = append(initiate, initBox...)
	if err := zmtpWriteFrame(rw, 0x04, initiate); err != nil {
		return nil, fmt.Errorf("zmtp curve client: send INITIATE: %w", err)
	}

	// ── 4. READY (from server) ───────────────────────────────────────────────────
	// Body: "\x05READY"(6) + nonce-suffix(8) + box
	_, readyBody, err := zmtpReadRawFrame(rw)
	if err != nil {
		return nil, fmt.Errorf("zmtp curve client: read READY: %w", err)
	}
	if len(readyBody) < 6+8+box.Overhead {
		return nil, fmt.Errorf("zmtp curve client: READY too short")
	}
	if string(readyBody[:6]) != "\x05READY" {
		return nil, fmt.Errorf("zmtp curve client: expected READY command")
	}
	readyNonce := nonce24(cNonceServer, readyBody[6:14])
	if _, ok = box.Open(nil, readyBody[14:], &readyNonce, &stPub, ctSec); !ok {
		return nil, fmt.Errorf("zmtp curve client: decrypt READY failed")
	}

	return newCurveSession(false, *ctSec, stPub), nil
}

// ─── Server handshake ──────────────────────────────────────────────────────────

func zmtpCURVEServer(rw net.Conn, sockType SocketType, serverKey CurveKey) (*curveSession, error) {
	// ── 1. HELLO ────────────────────────────────────────────────────────────────
	// Body: "\x05HELLO"(6) + version(2) + padding(70) + ctPub(32) + box(80) = 190
	_, helloBody, err := zmtpReadRawFrame(rw)
	if err != nil {
		return nil, fmt.Errorf("zmtp curve server: read HELLO: %w", err)
	}
	if len(helloBody) < 190 {
		return nil, fmt.Errorf("zmtp curve server: HELLO too short (%d bytes)", len(helloBody))
	}
	if string(helloBody[:6]) != "\x05HELLO" {
		return nil, fmt.Errorf("zmtp curve server: expected HELLO, got %q", helloBody[:6])
	}
	var ctPub [32]byte
	copy(ctPub[:], helloBody[78:110]) // offset: 6 (name) + 2 (version) + 70 (padding)
	helloNonce := nonce24u64(cNonceHello, 1)
	// The box contains 64 zeros — we just verify decryption succeeds.
	if _, ok := box.Open(nil, helloBody[110:], &helloNonce, &ctPub, &serverKey.Secret); !ok {
		return nil, fmt.Errorf("zmtp curve server: HELLO verification failed")
	}

	// Generate server ephemeral keypair S'/s'.
	stPub, stSec, err := box.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("zmtp curve server: generate transient key: %w", err)
	}

	// ── 2. WELCOME ──────────────────────────────────────────────────────────────
	// Cookie = secretbox[ctPub+stPub](cookie-key, "COOKIE--"+16-byte-random)
	// Allows stateless servers; we store state in-goroutine so this is optional,
	// but the client must echo the cookie bytes back in INITIATE.
	cookieKey, err := randN(32)
	if err != nil {
		return nil, fmt.Errorf("zmtp curve server: random cookie key: %w", err)
	}
	cookieNonceSuf, err := randN(16)
	if err != nil {
		return nil, fmt.Errorf("zmtp curve server: random cookie nonce: %w", err)
	}
	cookieNonce := nonce24(cNonceCookie, cookieNonceSuf)
	cookiePlain := append(ctPub[:], stPub[:]...) // 64 bytes
	var ck [32]byte
	copy(ck[:], cookieKey)
	cookieBox := secretbox.Seal(nil, cookiePlain, &cookieNonce, &ck)
	// cookie = nonce-suffix(16) + box(64+16=80) = 96 bytes
	cookie := append(cookieNonceSuf, cookieBox...)

	// WELCOME box plaintext: stPub(32) + cookie(96) = 128 bytes
	welcomePlain := append(stPub[:], cookie...)
	welcomeNonceSuf, err := randN(16)
	if err != nil {
		return nil, fmt.Errorf("zmtp curve server: random welcome nonce: %w", err)
	}
	welcomeNonce := nonce24(cNonceWelcome, welcomeNonceSuf)
	// Sealed with server permanent key → client transient pubkey.
	welcomeBox := box.Seal(nil, welcomePlain, &welcomeNonce, &ctPub, &serverKey.Secret)

	welcome := make([]byte, 0, 8+16+len(welcomeBox))
	welcome = append(welcome, '\x07', 'W', 'E', 'L', 'C', 'O', 'M', 'E')
	welcome = append(welcome, welcomeNonceSuf...)
	welcome = append(welcome, welcomeBox...)
	if err := zmtpWriteFrame(rw, 0x04, welcome); err != nil {
		return nil, fmt.Errorf("zmtp curve server: send WELCOME: %w", err)
	}

	// ── 3. INITIATE ─────────────────────────────────────────────────────────────
	// Body: "\x08INITIATE"(9) + cookie(96) + nonce-suffix(8) + box
	_, initiateBody, err := zmtpReadRawFrame(rw)
	if err != nil {
		return nil, fmt.Errorf("zmtp curve server: read INITIATE: %w", err)
	}
	if len(initiateBody) < 9+96+8+box.Overhead {
		return nil, fmt.Errorf("zmtp curve server: INITIATE too short")
	}
	if string(initiateBody[:9]) != "\x08INITIATE" {
		return nil, fmt.Errorf("zmtp curve server: expected INITIATE command")
	}
	// Cookie (bytes 9-104) is echoed back; we skip validation since we hold state.
	initNonce := nonce24(cNonceInitiate, initiateBody[105:113])
	// Sealed by client transient key → server transient key.
	initPlain, ok := box.Open(nil, initiateBody[113:], &initNonce, &ctPub, stSec)
	if !ok {
		return nil, fmt.Errorf("zmtp curve server: decrypt INITIATE failed")
	}
	// initPlain: clientPermPK(32) + vouchNonceSuf(16) + vouchBox(48) + metadata
	if len(initPlain) < 32+64 {
		return nil, fmt.Errorf("zmtp curve server: INITIATE plaintext too short")
	}
	var clientPermPK [32]byte
	copy(clientPermPK[:], initPlain[:32])

	// Verify vouch: Box[ctPub](c→S', "VOUCH---"+nonce-suffix)
	vouchNonce := nonce24(cNonceVouch, initPlain[32:48])
	vouchedPK, ok := box.Open(nil, initPlain[48:96], &vouchNonce, &clientPermPK, stSec)
	if !ok {
		return nil, fmt.Errorf("zmtp curve server: vouch verification failed")
	}
	if len(vouchedPK) < 32 {
		return nil, fmt.Errorf("zmtp curve server: vouch plaintext too short")
	}
	var vouchedCtPub [32]byte
	copy(vouchedCtPub[:], vouchedPK)
	if vouchedCtPub != ctPub {
		return nil, fmt.Errorf("zmtp curve server: vouch key mismatch")
	}

	// ── 4. READY ─────────────────────────────────────────────────────────────────
	metadata := zmtpEncodeProperty("Socket-Type", string(sockType))
	var readyNonceSuf [8]byte
	binary.BigEndian.PutUint64(readyNonceSuf[:], 1)
	readyNonce := nonce24(cNonceServer, readyNonceSuf[:])
	readyBox := box.Seal(nil, metadata, &readyNonce, &ctPub, stSec)

	ready := make([]byte, 0, 6+8+len(readyBox))
	ready = append(ready, '\x05', 'R', 'E', 'A', 'D', 'Y')
	ready = append(ready, readyNonceSuf[:]...)
	ready = append(ready, readyBox...)
	if err := zmtpWriteFrame(rw, 0x04, ready); err != nil {
		return nil, fmt.Errorf("zmtp curve server: send READY: %w", err)
	}

	return newCurveSession(true, *stSec, ctPub), nil
}

// ─── Post-handshake message encryption ────────────────────────────────────────

// zmtpCURVESendMsg encrypts each frame of msg as a CURVE MESSAGE command and
// writes it to w.  The MORE flag is encoded inside the encrypted payload so
// it is authenticated along with the frame data.
func zmtpCURVESendMsg(w io.Writer, msg Msg, cs *curveSession) error {
	n := len(msg.Frames)
	var prefix string
	if cs.isServer {
		prefix = cNonceServer
	} else {
		prefix = cNonceClient
	}
	for i, frame := range msg.Frames {
		moreFlag := byte(0)
		if i < n-1 {
			moreFlag = 0x01
		}
		counter := cs.sendCtr.Add(1)
		msgNonce := nonce24u64(prefix, counter)

		plain := append([]byte{moreFlag}, frame...)
		cipher := box.SealAfterPrecomputation(nil, plain, &msgNonce, &cs.sharedKey)

		// MESSAGE command body: "\x07MESSAGE" + 8-byte counter + ciphertext
		body := make([]byte, 0, 8+8+len(cipher))
		body = append(body, '\x07', 'M', 'E', 'S', 'S', 'A', 'G', 'E')
		body = append(body, msgNonce[16:]...) // last 8 bytes = counter suffix
		body = append(body, cipher...)
		if err := zmtpWriteFrame(w, 0x04, body); err != nil {
			return fmt.Errorf("zmtp curve: send MESSAGE frame %d/%d: %w", i+1, n, err)
		}
	}
	return nil
}

// zmtpCURVEReadMsg reads and decrypts CURVE MESSAGE commands until a complete
// ZMQ message is assembled.  Non-MESSAGE commands (e.g. PING) are silently
// skipped, matching the NULL mechanism's behaviour.
func zmtpCURVEReadMsg(r io.Reader, cs *curveSession) Msg {
	var msg Msg
	// Receive nonce prefix is the opposite side's send prefix.
	var prefix string
	if cs.isServer {
		prefix = cNonceClient
	} else {
		prefix = cNonceServer
	}
	for {
		_, body, err := zmtpReadRawFrame(r)
		if err != nil {
			msg.err = err
			return msg
		}
		// Skip frames that are not MESSAGE commands.
		if len(body) < 8 || string(body[:8]) != "\x07MESSAGE" {
			continue
		}
		if len(body) < 8+8+box.Overhead {
			msg.err = fmt.Errorf("zmtp curve: MESSAGE frame too short")
			return msg
		}
		msgNonce := nonce24(prefix, body[8:16]) // 8-byte counter from wire
		plain, ok := box.OpenAfterPrecomputation(nil, body[16:], &msgNonce, &cs.sharedKey)
		if !ok {
			msg.err = fmt.Errorf("zmtp curve: decrypt MESSAGE failed (nonce counter mismatch or tampering)")
			return msg
		}
		moreFlag := plain[0]
		msg.Frames = append(msg.Frames, plain[1:])
		if moreFlag&0x01 == 0 {
			return msg
		}
	}
}
