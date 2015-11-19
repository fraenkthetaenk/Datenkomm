package edu.hm.dako.chat.tcp;

import edu.hm.dako.chat.connection.Connection;
import edu.hm.dako.chat.connection.ConnectionFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class TcpConnectionFactory implements ConnectionFactory {
	
	private static Log log = LogFactory.getLog(TcpConnectionFactory.class);

	// Test: Zaehlt die Verbindungsaufbauversuche, bis eine Verbindung vom Server angenommen wird
	private long connectionTryCounter = 0;

	public Connection connectToServer(String remoteServerAddress,
			int serverPort, int localPort, int sendBufferSize, int receiveBufferSize) throws IOException {

		TcpConnection connection = null;
		boolean connected = false;
		InetAddress localAddress = null; // Es wird "localhost" fuer die lokale IP-Adresse verwendet
		
		while (!connected) {
			try {
				 				
				connectionTryCounter++;
				
				
				connection = new TcpConnection(new Socket(remoteServerAddress, serverPort, localAddress, localPort), 
					sendBufferSize, receiveBufferSize, false, true);
				
				connected = true;
				
			} catch (BindException e) {
				
				// Lokaler Port schon verwendet
				log.error("BindException beim Verbindungsaufbau: " + e.getMessage());			
				// try again
				
			} catch (IOException e) {
				
				log.error("IOException beim Verbindungsaufbau: " + e.getMessage());			
				// try again
			
			} catch (Exception e) {
				

				log.error("Sonstige Exception beim Verbindungsaufbau " + e.getMessage());			
				// try again
			}
		}

		log.debug("Anzahl der Verbindungsaufbauversuche für die Verbindung zum Server: " + connectionTryCounter);	
		return connection;
	}

}
