import java.util.*;

public class PeerConfig {

    private final String peerId;
    private final String hostName;
    private final int port;
    private final boolean hasFile;

    // Derived from CommonConfig at construction time
    private final int numPieces;
    private final int pieceSize;
    private final int fileSize;
    private final String fileName;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * @param myPeerInfo  The PeerInfo entry for this peer (from PeerInfoConfig)
     * @param common      The loaded CommonConfig
     */
    public PeerConfig(PeerInfo myPeerInfo, CommonConfig common) {
        this.peerId   = myPeerInfo.getPeerId();
        this.hostName = myPeerInfo.getHostName();
        this.port     = myPeerInfo.getPort();
        this.hasFile  = myPeerInfo.File();

        this.fileSize  = common.getFileSize();
        this.pieceSize = common.getPieceSize();
        this.fileName  = common.getFileName();
        this.numPieces = common.getNumPieces();
    }

    // -------------------------------------------------------------------------
    // Identity
    // -------------------------------------------------------------------------

    public String getPeerId() {
        return peerId;
    }

    public String getHostName() {
        return hostName;
    }

    public int getPort() {
        return port;
    }

    /** True if this peer starts with the complete file. */
    public boolean hasFile() {
        return hasFile;
    }

    // -------------------------------------------------------------------------
    // File / piece info
    // -------------------------------------------------------------------------

    public String getFileName() {
        return fileName;
    }

    public int getFileSize() {
        return fileSize;
    }

    public int getPieceSize() {
        return pieceSize;
    }

    public int getNumPieces() {
        return numPieces;
    }

    /**
     * Returns the actual size in bytes of a specific piece.
     * All pieces are pieceSize bytes except the last, which may be smaller.
     */
    public int getPieceSizeForIndex(int pieceIndex) {
        if (pieceIndex < numPieces - 1) {
            return pieceSize;
        }
        int remainder = fileSize % pieceSize;
        return (remainder == 0) ? pieceSize : remainder;
    }

    /**
     * Returns the number of bytes needed for the bitfield array.
     * Spare bits at the end are set to zero per the protocol spec.
     */
    public int getBitfieldByteLength() {
        return (numPieces + 7) / 8;
    }

    // -------------------------------------------------------------------------
    // Path helpers
    // -------------------------------------------------------------------------

    /**
     * Returns the subdirectory for this peer, e.g. "peer_1001".
     * Files specific to this peer (complete or partial) go here.
     */
    public String getPeerDirectory() {
        return "peer_" + peerId;
    }

    /**
     * Returns the full relative path to this peer's copy of the shared file,
     * e.g. "peer_1001/TheFile.dat".
     */
    public String getFilePath() {
        return getPeerDirectory() + "/" + fileName;
    }

    /**
     * Returns the log file path for this peer, e.g. "log_peer_1001.log".
     */
    public String getLogFilePath() {
        return "log_peer_" + peerId + ".log";
    }

    // -------------------------------------------------------------------------
    // Debug
    // -------------------------------------------------------------------------

    @Override
    public String toString() {
        return "PeerConfig{"
            + "peerId='" + peerId + '\''
            + ", host='" + hostName + '\''
            + ", port=" + port
            + ", hasFile=" + hasFile
            + ", fileName='" + fileName + '\''
            + ", fileSize=" + fileSize
            + ", pieceSize=" + pieceSize
            + ", numPieces=" + numPieces
            + '}';
    }
}