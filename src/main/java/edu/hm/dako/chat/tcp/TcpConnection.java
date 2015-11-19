package edu.hm.dako.chat.tcp;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.SocketException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.hm.dako.chat.connection.Connection;

public class TcpConnection implements Connection {
	
    private static Log log = LogFactory.getLog(TcpConnection.class);

    // Ein- und Ausgabestrom der Verbindung
    private final ObjectOutputStream out;
    private final ObjectInputStream in;
    
    // Verwendetes TCP-Socket
    private final Socket socket;
    
    /*
     *  Zur Information:
     *  Standardgroesse des Empfangspuffers einer TCP-Verbindung: 8192 Byte
     *  Standardgroesse des Sendepuffers einer TCP-Verbindung: 8192 Byte 
     */
    public TcpConnection(Socket socket, int sendBufferSize, int receiveBufferSize, boolean keepAlive, boolean TcpNoDelay) {
    	
    	this.socket = socket;
        
        log.debug(Thread.currentThread().getName() + ": Verbindung mit neuem Client aufgebaut, Remote-TCP-Port " +
                socket.getPort());
        
        try {
            // Ein- und Ausgabe-Objektstroeme erzeugen
        	// Achtung: Erst Ausgabestrom, dann Eingabestrom erzeugen, sonst Fehler beim Verbindungsaufbau, siehe API-Beschreibung
        	
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            
            log.debug("Standardgroesse des Empfangspuffers der Verbindung: " + socket.getReceiveBufferSize() + " Byte");
            log.debug("Standardgroesse des Sendepuffers der Verbindung: " + socket.getSendBufferSize() + " Byte");
            socket.setReceiveBufferSize(receiveBufferSize);
            socket.setSendBufferSize(sendBufferSize);
            log.debug("Eingestellte Groesse des Empfangspuffers der Verbindung: " + socket.getReceiveBufferSize() + " Byte");
            log.debug("Eingestellte Groesse des Sendepuffers der Verbindung: " + socket.getSendBufferSize() + " Byte");
            
            // TCP-Optionen einstellen
            socket.setTcpNoDelay(TcpNoDelay);	
            //socket.setTcpNoDelay(false);	
            socket.setKeepAlive(keepAlive);
            
            // TCP-Optionen ausgeben
            if (socket.getKeepAlive()) {
            	 log.debug("KeepAlive-Option ist fuer die Verbindung aktiviert");
            } else {
            	 log.debug("KeepAlive-Option ist fuer die Verbindung nicht aktiviert");
            }
            
            if (socket.getTcpNoDelay()) {
           	 	log.debug("Nagle-Algorithmus ist fuer die Verbindung aktiviert");
            } else {
           	 	log.debug("Nagle-Algorithmus ist fuer die Verbindung nicht aktiviert");
            }
            
        } catch (SocketException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Serializable receive(int timeout) throws Exception {

    	// Exceptions beim Lesen aus einem Stream sind immer schwerwiegend und ein Recovery des Streams nicht moeglich 
    	Object message = in.readObject();
    	return (Serializable) message;
    }
    
    
    @Override
    public Serializable receive() throws Exception {
      	// Exceptions beim Lesen aus einem Stream sind immer schwerwiegend und ein Recovery des Streams nicht moeglich 
        Object message = in.readObject();
    	return (Serializable) message;
    }

    @Override
    public void send(Serializable message) throws Exception {
    	
    	if (socket.isClosed()) {
    		
    		log.debug("Sendeversuch, obwohl Socket geschlossen ist");
    		return;
    	}
    	if (!socket.isConnected()) { 
    		
    		log.debug("Sendeversuch, obwohl Verbindung nicht mehr steht");
     		return;
    	}
    	
    	try {
    		// Exceptions beim Schreiben in einen Stream sind immer schwerwiegend und ein Recovery des Streams nicht moeglich 
    		out.writeObject(message);
    		out.flush();
    	} catch (Exception e) {
    		log.debug("Exception beim Sendeversuch an " + socket.getInetAddress());	
    		log.debug(e.getMessage());
    		//throw e;
    	}
    }

    @Override
    public synchronized void close() throws IOException {
        out.flush();
        log.debug("Verbindungssocket wird geschlossen, lokaler Port: " 
        		+ socket.getLocalPort() + ", entfernter Port: " + socket.getPort());
        socket.close();
    }
}
