package edu.hm.dako.chat.client;


import edu.hm.dako.chat.common.SharedClientStatistics;
import edu.hm.dako.chat.connection.ConnectionFactory;
import edu.hm.dako.chat.connection.DecoratingConnectionFactory;
import edu.hm.dako.chat.tcp.TcpChatAdvanceClientImpl;
import edu.hm.dako.chat.tcp.TcpChatSimpleClientImpl;
import edu.hm.dako.chat.tcp.TcpConnectionFactory;
import edu.hm.dako.chat.benchmarking.UserInterfaceInputParameters;

/**
 * Uebernimmt die Konfiguration und die Erzeugung bestimmter Client-Typen.
 * Siehe {@link edu.hm.dako.echo.benchmarking.UserInterfaceInputParameters.ImplementationType}
 * Dies beinhaltet die {@link ConnectionFactory}, die Adressen, Ports, Denkzeit etc.
 */
public final class ClientFactory {

	private ClientFactory() {
	}

	public static Runnable getClient(ChatClientUserInterface userInterface, UserInterfaceInputParameters param, int numberOfClient, SharedClientStatistics sharedData) {
		try {
			switch (param.getImplementationType()) {
				case TCPImplementation:
					return new TcpChatAdvanceClientImpl(userInterface, param.getRemoteServerPort(),
							param.getRemoteServerAddress(), numberOfClient, param.getMessageLength(),
							param.getNumberOfMessages(), param.getClientThinkTime(),
							param.getNumberOfRetries(), param.getResponseTimeout(),
							sharedData, getDecoratedFactory(new TcpConnectionFactory()));
	            /*
                 case UDPImplementation:
                 
                    return new UdpChatClientImpl(param.getRemoteServerPort(),
                            param.getRemoteServerAddress(), numberOfClient, param.getMessageLength(),
                            param.getNumberOfMessages(), param.getClientThinkTime(),
                            param.getNumberOfRetries(), param.getResponseTimeout(),
                            sharedData, getDecoratedFactory(new UdpClientConnectionFactory()));
                */

				default:
					throw new RuntimeException("Unbekannter Implementierungstyp: " + param.getImplementationType());
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static ConnectionFactory getDecoratedFactory(ConnectionFactory connectionFactory) {
		return new DecoratingConnectionFactory(connectionFactory);
	}
}