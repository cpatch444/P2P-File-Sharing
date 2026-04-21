.PHONY: all clean peer setup submit

SOURCES := $(wildcard *.java)
CLASSES := $(SOURCES:.java=.class)

all: $(CLASSES)

%.class: %.java
	javac $<

# Run a peer: make peer ID=1001
peer:
	@if [ -z "$(ID)" ]; then echo "Usage: make peer ID=<peerID>" >&2; exit 1; fi
	java PeerProcess $(ID)

# Pre-create peer_<id>/ subdirectories for every peer in PeerInfo.cfg
setup:
	@awk 'NF>=4 {print "peer_" $$1}' PeerInfo.cfg | xargs -r mkdir -p

clean:
	rm -f *.class log_peer_*.log

# Build a submission tarball with only source/build/run files (no configs or binaries)
submit: clean
	tar cvf p2p-project.tar *.java Makefile startRemotePeers.sh README.md
