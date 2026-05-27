#!/usr/bin/env bash
# compile_thesis.sh — regenerate all thesis figures then compile the PDF.
#
# Usage:
#   ./compile_thesis.sh               # regenerate plots + full LaTeX compile
#   ./compile_thesis.sh --plots-only  # regenerate plots only, no LaTeX
#   ./compile_thesis.sh --latex-only  # skip plot regeneration, just compile
#   ./compile_thesis.sh --help | -h   # show this help
#
# Prerequisites:
#   python3, matplotlib, numpy   (for plots)
#   texlive-full or equivalent   (for pdflatex + bibtex)
#
# After a benchmark run, results land in:
#   benchmarks/scenarios/results/       (dev/Docker mode)
#   benchmarks/scenarios/prod/results/  (prod/Mininet mode)
#   benchmarks/scenarios/phys/results/  (phys/Pi mode)
#
# generate_plots.py reads these automatically when present.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
THESIS_DIR="$REPO_ROOT/thesis"
FIGURES_DIR="$THESIS_DIR/figures"
GENERATE_PLOTS="$FIGURES_DIR/generate_plots.py"
THESIS_TEX="$THESIS_DIR/thesis.tex"

# ── Colour helpers ────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'
BOLD='\033[1m'; RESET='\033[0m'

log()     { printf "${CYAN}[compile]${RESET} %s\n" "$*"; }
success() { printf "${GREEN}[OK]${RESET}     %s\n" "$*"; }
die()     { printf "${RED}[ERR]${RESET}    %s\n" "$*" >&2; exit 1; }

# ── Argument parsing ──────────────────────────────────────────────────────────
DO_PLOTS=true
DO_LATEX=true

for arg in "$@"; do
    case "$arg" in
    --plots-only) DO_LATEX=false ;;
    --latex-only) DO_PLOTS=false ;;
    --help|-h)
        sed -n '2,20p' "$0" | sed 's/^# \?//'
        exit 0
        ;;
    *) die "Unknown argument: $arg" ;;
    esac
done

# ── Plot regeneration ─────────────────────────────────────────────────────────
if $DO_PLOTS; then
    log "Checking Python dependencies…"
    python3 -c "import matplotlib, numpy" 2>/dev/null \
        || die "Missing Python packages. Install with: pip3 install matplotlib numpy"

    log "Regenerating thesis figures…"
    cd "$REPO_ROOT"
    python3 "$GENERATE_PLOTS"
    success "Figures written to $FIGURES_DIR/"
fi

# ── LaTeX compilation ─────────────────────────────────────────────────────────
if $DO_LATEX; then
    command -v pdflatex >/dev/null 2>&1 \
        || die "pdflatex not found. Install texlive: sudo apt-get install -y texlive-full"
    command -v bibtex >/dev/null 2>&1 \
        || die "bibtex not found. Install texlive: sudo apt-get install -y texlive-full"

    cd "$THESIS_DIR"

    log "LaTeX pass 1 / 3…"
    pdflatex -interaction=nonstopmode -halt-on-error thesis.tex \
        | grep -E '^(!)|\(.*\.tex\)' || true

    log "BibTeX…"
    bibtex thesis 2>&1 | grep -v '^This is BibTeX' | grep -v '^The top-level' || true

    log "LaTeX pass 2 / 3…"
    pdflatex -interaction=nonstopmode -halt-on-error thesis.tex \
        | grep -E '^(!)' || true

    log "LaTeX pass 3 / 3…"
    pdflatex -interaction=nonstopmode -halt-on-error thesis.tex \
        | grep -E '^(!)' || true

    success "Thesis compiled → $THESIS_DIR/thesis.pdf"
fi
