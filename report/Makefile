MAIN    := coursework
LATEX   := pdflatex
BIBER   := biber
PLANTUML:= plantuml
LFLAGS  := -interaction=nonstopmode -halt-on-error

SRC     := $(MAIN).tex $(wildcard *.tex) refs.bib

# PlantUML diagrams. Output .png is named after the diagram (@startuml <name>),
# which matches the source file for all but c4_container (special-cased below).
DIAGRAMS:= task_state.png hash_chain.png sandbox_layers.png \
           sequence_task.png C4_Container_BOINC_Quota.png

.PHONY: all clean distclean view diagrams

all: $(MAIN).pdf

diagrams: $(DIAGRAMS)

# Render a .puml to a same-named .png via PlantUML
%.png: %.puml
	$(PLANTUML) -tpng $<

# c4_container.puml declares @startuml C4_Container_BOINC_Quota -> different name
C4_Container_BOINC_Quota.png: c4_container.puml
	$(PLANTUML) -tpng $<

# Full build: latex -> biber -> latex -> latex (resolve refs/citations)
$(MAIN).pdf: $(SRC) $(DIAGRAMS)
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
