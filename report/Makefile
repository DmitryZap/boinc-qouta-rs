MAIN    := coursework
LATEX   := pdflatex
BIBER   := biber
LFLAGS  := -interaction=nonstopmode -halt-on-error

SRC     := $(MAIN).tex $(wildcard *.tex) refs.bib

.PHONY: all clean distclean view

all: $(MAIN).pdf

# Full build: latex -> biber -> latex -> latex (resolve refs/citations)
$(MAIN).pdf: $(SRC)
	$(LATEX) $(LFLAGS) $(MAIN)
	$(BIBER) $(MAIN)
	$(LATEX) $(LFLAGS) $(MAIN)
	$(LATEX) $(LFLAGS) $(MAIN)

view: $(MAIN).pdf
	open $(MAIN).pdf

# Remove build intermediates, keep PDF
clean:
	rm -f $(MAIN).aux $(MAIN).bbl $(MAIN).bcf $(MAIN).blg \
	      $(MAIN).log $(MAIN).out $(MAIN).run.xml $(MAIN).toc

# Remove everything including PDF
distclean: clean
	rm -f $(MAIN).pdf
