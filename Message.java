import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

final class MessageTypes {
    public static final int CHOKE = 0;
    public static final int UNCHOKE = 1;
    public static final int INTERESTED = 2;
    public static final int NOT_INTERESTED = 3;
    public static final int HAVE = 4;
    public static final int BITFIELD = 5;
    public static final int REQUEST = 6;
    public static final int PIECE = 7;

    private MessageTypes() {}
}

public class Message {
    private final int type;
    private final byte[] payload;

    public Message(int type, byte[] payload) {
        this.type = type;
        this.payload = payload;
    }

    public Message(int type) {
        this.type = type;
        this.payload = new byte[0];
    }

    public int getType() {
        return type;
    }

    public byte[] getPayload() {
        return payload;
    }

    public static Message choke() {
        return new Message(MessageTypes.CHOKE);
    }

    public static Message unchoke() {
        return new Message(MessageTypes.UNCHOKE);
    }

    public static Message interested() {
        return new Message(MessageTypes.INTERESTED);
    }

    public static Message notInterested() {
        return new Message(MessageTypes.NOT_INTERESTED);
    }

    public static Message have(int pieceIndex) {
        byte[] payload = intToBytes(pieceIndex);
        return new Message(MessageTypes.HAVE, payload);
    }

    public static Message bitfield(byte[] bitfield) {
        return new Message(MessageTypes.BITFIELD, bitfield);
    }

    public static Message request(int pieceIndex) {
        byte[] payload = intToBytes(pieceIndex);
        return new Message(MessageTypes.REQUEST, payload);
    }

    public static Message piece(int pieceIndex, byte[] content) {
        byte[] payload = new byte[4 + content.length];
        byte[] indexBytes = intToBytes(pieceIndex);
        payload[0] = indexBytes[0];
        payload[1] = indexBytes[1];
        payload[2] = indexBytes[2];
        payload[3] = indexBytes[3];
        for (int i = 0; i < content.length; i++) {
            payload[4 + i] = content[i];
        }
        return new Message(MessageTypes.PIECE, payload);
    }

    // payload helpers

    public int getPieceIndex() {
        return bytesToInt(payload, 0);
    }

    public byte[] getPieceContent() {
        byte[] content = new byte[payload.length - 4];
        for (int i = 0; i < content.length; i++) {
            content[i] = payload[4 + i];
        }
        return content;
    }

    // wire format

    public void send(OutputStream out) throws IOException {
        int msgLength = 1 + payload.length;
        byte[] lengthBytes = intToBytes(msgLength);
        out.write(lengthBytes);
        out.write(type);
        if (payload.length > 0) {
            out.write(payload);
        }
        out.flush();
    }

    public static Message receive(InputStream in) throws IOException {
        byte[] lengthBytes = readExact(in, 4);
        int msgLength = bytesToInt(lengthBytes, 0);

        if (msgLength < 1) {
            throw new IOException("Invalid message length: " + msgLength);
        }

        int msgType = in.read();
        if (msgType == -1) {
            throw new IOException("Connection closed while reading message type");
        }

        byte[] payload = new byte[0];
        if (msgLength > 1) {
            payload = readExact(in, msgLength - 1);
        }

        return new Message(msgType, payload);
    }

    // byte utilities

    private static byte[] intToBytes(int value) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (value >> 24);
        bytes[1] = (byte) (value >> 16);
        bytes[2] = (byte) (value >> 8);
        bytes[3] = (byte) value;
        return bytes;
    }

    private static int bytesToInt(byte[] data, int offset) {
        int b1 = (data[offset] & 0xFF) << 24;
        int b2 = (data[offset + 1] & 0xFF) << 16;
        int b3 = (data[offset + 2] & 0xFF) << 8;
        int b4 = (data[offset + 3] & 0xFF);
        return b1 | b2 | b3 | b4;
    }

    private static byte[] readExact(InputStream in, int numBytes) throws IOException {
        byte[] data = new byte[numBytes];
        int bytesRead = 0;
        while (bytesRead < numBytes) {
            int read = in.read(data, bytesRead, numBytes - bytesRead);
            if (read == -1) {
                throw new IOException("Connection closed, expected " + numBytes + " bytes");
            }
            bytesRead += read;
        }
        return data;
    }
}
