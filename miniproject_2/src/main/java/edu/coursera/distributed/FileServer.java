package edu.coursera.distributed;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * A basic and very limited implementation of a file server that responds to GET
 * requests from HTTP clients.
 */
public final class FileServer {
    /**
     * Main entrypoint for the basic file server.
     *
     * @param socket Provided socket to accept connections on.
     * @param fs A proxy filesystem to serve files from. See the PCDPFilesystem
     *           class for more detailed documentation of its usage.
     * @throws IOException If an I/O error is detected on the server. This
     *                     should be a fatal error, your file server
     *                     implementation is not expected to ever throw
     *                     IOExceptions during normal operation.
     */
    public void run(final ServerSocket socket, final PCDPFilesystem fs)
            throws IOException {
        /*
         * Enter a spin loop for handling client requests to the provided
         * ServerSocket object.
         */
        while (true) {

            // TODO Delete this once you start working on your solution.
            //throw new UnsupportedOperationException();

            // TODO 1) Use socket.accept to get a Socket object
            Socket s = socket.accept();
            /*
             * TODO 2) Using Socket.getInputStream(), parse the received HTTP
             * packet. In particular, we are interested in confirming this
             * message is a GET and parsing out the path to the file we are
             * GETing. Recall that for GET HTTP packets, the first line of the
             * received packet will look something like:
             *
             *     GET /path/to/file HTTP/1.1
             */
            Scanner scanner = new Scanner(s.getInputStream());
            String path = null;
            if (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (line != null) {
                    String[] parts = line.split("\\s");
                    if (parts.length == 3
                        && parts[0].equals("GET")
                        && parts[2].equals("HTTP/1.1")) {
                        path = parts[1];
                    }
                }
            }
            //scanner.close();
            if (path == null) {
                serveResponse(s, 400, "Bad Request", null);
                return;
            }

            /*
             * TODO 3) Using the parsed path to the target file, construct an
             * HTTP reply and write it to Socket.getOutputStream(). If the file
             * exists, the HTTP reply should be formatted as follows:
             *
             *   HTTP/1.0 200 OK\r\n
             *   Server: FileServer\r\n
             *   \r\n
             *   FILE CONTENTS HERE\r\n
             *
             * If the specified file does not exist, you should return a reply
             * with an error code 404 Not Found. This reply should be formatted
             * as:
             *
             *   HTTP/1.0 404 Not Found\r\n
             *   Server: FileServer\r\n
             *   \r\n
             *
             * Don't forget to close the output stream.
             */
            String content = fs.readFile(new PCDPPath(path));
            if (content != null) {
                serveResponse(s, 200, "OK", content);
            } else {
                serveResponse(s, 404, "Not Found", null);
            }

            s.close();
        }
    }
    private void serveResponse(Socket s, int code,
                               String codeName, String content) throws IOException {
        try {
            OutputStream o = s.getOutputStream();
            o.write(String.format("HTTP/1.0 %d %s\r\n", code, codeName).getBytes(StandardCharsets.UTF_8));
            o.write("Server: FileServer\r\n".getBytes(StandardCharsets.UTF_8));
            o.write("\r\n".getBytes(StandardCharsets.UTF_8));
            if (content != null) {
                o.write(content.getBytes(StandardCharsets.UTF_8));
                o.write('\r');
                o.write('\n');
            }
            o.flush();
            o.close();
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
            e.printStackTrace(new PrintWriter(System.err));
        }
    }
}
