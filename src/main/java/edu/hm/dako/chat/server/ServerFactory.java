package edu.hm.dako.chat.server;

import edu.hm.dako.chat.connection.Connection;
import edu.hm.dako.chat.connection.LoggingConnectionDecorator;
import edu.hm.dako.chat.connection.ServerSocket;
import edu.hm.dako.chat.tcp.TcpChatSimpleServerImpl;
import edu.hm.dako.chat.tcp.TcpServerSocket;
import edu.hm.dako.chat.benchmarking.UserInterfaceInputParameters;

import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

/**
 * Uebernimmt die Konfiguration und Erzeugung bestimmter Server-Typen.
 * Siehe {@link edu.hm.dako.echo.benchmarking.UserInterfaceInputParameters.ImplementationType}
 * Dies beinhaltet Art des Servers und Konfiguration dessen Thread-Pool.
 */
public final class ServerFactory {
    private static Log log = LogFactory.getLog(ServerFactory.class);
    private static final int DEFAULT_SERVER_PORT = 50000; // Standard-Port des Servers
    /*
     * Die Groesse des Empfangspuffers ist fuer den Server sehr wichtig. 
     * Um viele parallele Client-Threads zu bedienen, sollte der Empfangspuffer im Server gut ausgetestet werden,
     * also so gross wie noetig sein, aber nicht zu gross, da sonst Speicher verschwendet wird, da alle Requests 
     * ueber ein Socket empfangen werden. Bei einer TCP-Loesung ist das nicht so problematisch, da jeder Client 
     * ueber eine eigene Verbindung, also ueber ein eigenes Socket mit eigenem Empfangspuffer bedient wird.
     */
    private static final int SERVER_SEND_BUFFER_SIZE = 100000; // Sendepuffer des Servers in Byte
    private static final int SERVER_RECEIVE_BUFFER_SIZE = 100000; // Empfangspuffer des Servers in Byte
    
    private ServerFactory() {
    }

    public static ChatServer getServer(UserInterfaceInputParameters.ImplementationType type)
            throws Exception {
        log.debug("ChatServer (" + type.toString() + ") wird gestartet");
        switch (type) {
            case TCPImplementation:
                return new TcpChatSimpleServerImpl(Executors.newCachedThreadPool(), getDecoratedServerSocket(
                        new TcpServerSocket(DEFAULT_SERVER_PORT,
                        					SERVER_SEND_BUFFER_SIZE, 
                        					SERVER_RECEIVE_BUFFER_SIZE)));
            /*
            case UDPImplementation:
                return new UdpChatServerImpl(Executors.newCachedThreadPool(), getDecoratedServerSocket(
                        new UdpServerSocket(DEFAULT_SERVER_PORT, 
                        					SERVER_SEND_BUFFER_SIZE, 
                        					SERVER_RECEIVE_BUFFER_SIZE)));
            */
            default:
                throw new RuntimeException("Unknown type: " + type);
        }
    }
      
    private static ServerSocket getDecoratedServerSocket(ServerSocket serverSocket) {
        return new DecoratingServerSocket(serverSocket);
    }

    /**
     * Startet den ausgewaehlten Server.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

    	 PropertyConfigurator.configureAndWatch("log4j.server.properties", 60 * 1000);
    	/* Hinweis:
         * Im ImplementationType der naechsten Anweisungen muss der Server, 
    	 * der gestartet werden soll, angegeben werden
         */   	
    	getServer(UserInterfaceInputParameters.ImplementationType.TCPImplementation).start();
    	//getServer(UserInterfaceInputParameters.ImplementationType.UDPImplementation).start();
    }

    private static class DecoratingServerSocket implements ServerSocket {

        private final ServerSocket wrappedServerSocket;

        DecoratingServerSocket(ServerSocket wrappedServerSocket) {
            this.wrappedServerSocket = wrappedServerSocket;
        }

        @Override
        public Connection accept() throws Exception {
            return new LoggingConnectionDecorator(wrappedServerSocket.accept());
        }

        @Override
        public void close() throws Exception {
            wrappedServerSocket.close();
        }

        @Override
        public boolean isClosed() {
            return wrappedServerSocket.isClosed();
        }
    }
}
