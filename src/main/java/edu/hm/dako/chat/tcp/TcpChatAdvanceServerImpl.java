package edu.hm.dako.chat.tcp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.hm.dako.chat.common.ExceptionHandler;
import edu.hm.dako.chat.common.ChatClientConversationStatus;
import edu.hm.dako.chat.common.ChatClientListEntry;
import edu.hm.dako.chat.common.ChatPDU;
import edu.hm.dako.chat.common.SharedChatClientList;
import edu.hm.dako.chat.connection.Connection;
import edu.hm.dako.chat.connection.ServerSocket;
import edu.hm.dako.chat.server.ChatServer;

/**
 * <p/>
 * Chat-Server-Implementierung
 *
 * @author Mandl
 */
public class TcpChatAdvanceServerImpl implements ChatServer {

	private static Log log = LogFactory.getLog(TcpChatAdvanceServerImpl.class);

	// Threadpool fuer Woekerthreads
	private final ExecutorService executorService;

	// Socket fuer den Listener, der alle Verbindungsaufbauwuensche der Clients
	// entgegennimmt
	private ServerSocket socket;

	// Gemeinsam fuer alle Workerthreads verwaltete Liste aller eingeloggten
	// Clients
	private SharedChatClientList clients;

	// Startzeit fuer die RTT-Messung der Request-Bearbeitungsdauer eines
	// Clients
	private long startTime;

	// Zaehler fuer Logouts und gesendete Events nur zum Tests
	private static AtomicInteger logoutCounter = new AtomicInteger(0);
	private static AtomicInteger eventCounter = new AtomicInteger(0);

	public TcpChatAdvanceServerImpl(ExecutorService executorService,
			ServerSocket socket) {
		log.debug("TcpChatAdvanceServerImpl konstruiert");
		this.executorService = executorService;
		this.socket = socket;
	}

	@Override
	public void start() {

		clients = SharedChatClientList.getInstance(); // Clientliste erzeugen
		while (!Thread.currentThread().isInterrupted() && !socket.isClosed()) {
			try {
				// Auf ankommende Verbindungsaufbauwuensche warten
				System.out
						.println("AdvanceChatServer wartet auf Verbindungsanfragen von Clients...");
				Connection connection = socket.accept();
				log.debug("Neuer Verbindungsaufbauwunsch empfangen");

				// Neuen Workerthread starten
				executorService.submit(new ChatWorker(connection));
			} catch (Exception e) {
				log.error("Exception beim Entgegennehmen von Verbindungsaufbauwuenschen: "
						+ e);
				ExceptionHandler.logException(e);
			}
		}
	}

	@Override
	public void stop() throws Exception {
		System.out.println("AdvanceChatServer beendet sich");
		clients.deleteAll(); // Loeschen der Userliste
		Thread.currentThread().interrupt();
		socket.close();
		log.debug("Listen-Socket geschlossen");
		executorService.shutdown();
		try {
			executorService.awaitTermination(10, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			log.error("Das Beenden des ExecutorService wurde unterbrochen");
			ExceptionHandler.logExceptionAndTerminate(e);
		}
		log.debug("Threadpool freigegeben");
	}

	/**
	 * Worker-Thread zur serverseitigen Bedienung einer Session mit einem
	 * Client. Jedem Client wird serverseitig ein Worker-Thread zugeordnet.
	 * 
	 * @author Mandl
	 *
	 */
	private class ChatWorker implements Runnable {

		private Connection connection; // Verbindungs-Handle
		private boolean finished = false;
		private String userName; // Username des durch den Worker-Thread
									// bedienten Clients

		private ChatWorker(Connection con) {
			this.connection = con;
		}

		@Override
		public void run() {

			log.debug("ChatWorker-Thread erzeugt, Threadname: "
					+ Thread.currentThread().getName());
			while (!finished && !Thread.currentThread().isInterrupted()) {
				try {
					// Warte auf naechste Nachricht des Clients und fuehre
					// entsprechende Aktion aus
					handleIncomingMessage();
				} catch (Exception e) {
					log.error("Exception waehrend der Nachrichtenverarbeitung");
					ExceptionHandler.logException(e);
				}
			}
			log.debug(Thread.currentThread().getName() + " beendet sich");
			closeConnection();
		}

		/**
		 * Erzeugen einer Logout-Event-PDU
		 * 
		 * @param receivedPdu
		 *            Empfangene PDU (Logout-Request-PDU)
		 * @return Erzeugte PDU
		 */
		private ChatPDU createLogoutEventPdu(ChatPDU receivedPdu) {

			ChatPDU pdu = new ChatPDU();
			pdu.setPduType(ChatPDU.LOGOUT_EVENT);
			pdu.setUserName(userName);
			pdu.setEventUserName(userName);
			pdu.setServerThreadName(Thread.currentThread().getName());
			pdu.setClientThreadName(receivedPdu.getClientThreadName());
			pdu.setClientStatus(ChatClientConversationStatus.UNREGISTERING);
			eventCounter.incrementAndGet();
			return pdu;
		}

		/**
		 * Erzeugen einer Login-Event-PDU
		 * 
		 * @param receivedPdu
		 *            Empfangene PDU (Login-Request-PDU)
		 * @return Erzeugte PDU
		 */
		private ChatPDU createLoginEventPdu(ChatPDU receivedPdu) {

			ChatPDU pdu = new ChatPDU();
			pdu.setPduType(ChatPDU.LOGIN_EVENT);
			pdu.setServerThreadName(Thread.currentThread().getName());
			pdu.setClientThreadName(receivedPdu.getClientThreadName());
			pdu.setUserName(userName);
			pdu.setEventUserName(receivedPdu.getUserName());
			pdu.setUserName(receivedPdu.getUserName());
			pdu.setClientStatus(ChatClientConversationStatus.REGISTERING);
			eventCounter.incrementAndGet();
			return pdu;
		}

		/**
		 * Erzeugen einer Login-Response-PDU
		 * 
		 * @param receivedPdu
		 *            Empfangene PDU (Login-Request-PDU)
		 * @return Erzeugte PDU
		 */
		private ChatPDU createLoginResponsePdu(ChatPDU receivedPdu) {

			ChatPDU pdu = new ChatPDU();
			pdu.setPduType(ChatPDU.LOGIN_RESPONSE);
			pdu.setServerThreadName(Thread.currentThread().getName());
			pdu.setClientThreadName(receivedPdu.getClientThreadName());
			pdu.setUserName(receivedPdu.getEventUserName());

			ChatClientListEntry client = clients.getClient(receivedPdu
					.getUserName());
			if (client != null) {
				pdu.setClientStatus(client.getStatus());
				pdu.setServerTime(System.nanoTime() - client.getStartTime());
			} else {
				pdu.setClientStatus(ChatClientConversationStatus.REGISTERED);
			}
			return pdu;
		}

		/**
		 * Erzeugen einer Chat-Message-Event-PDU
		 * 
		 * @param receivedPdu
		 *            (Chat-Message-Request-PDU)
		 * @return Erzeugte PDU
		 */
		private ChatPDU createChatMessageEventPdu(ChatPDU receivedPdu) {

			ChatPDU pdu = new ChatPDU();
			pdu.setPduType(ChatPDU.CHAT_MESSAGE_EVENT);
			pdu.setServerThreadName(Thread.currentThread().getName());
			pdu.setClientThreadName(receivedPdu.getClientThreadName());
			pdu.setUserName(userName);
			pdu.setEventUserName(receivedPdu.getUserName());
			pdu.setSequenceNumber(receivedPdu.getSequenceNumber());
			pdu.setClientStatus(ChatClientConversationStatus.REGISTERED);
			pdu.setMessage(receivedPdu.getMessage());
			eventCounter.incrementAndGet();
			return pdu;
		}

		/**
		 * Erzeugen einer Chat-Message-Response-PDU
		 * 
		 * @param receivedPdu
		 *            (Chat-Message-Request-PDU)
		 * @return Erzeugte PDU
		 */
		private ChatPDU createChatMessageResponsePdu(ChatPDU receivedPdu) {

			ChatPDU pdu = new ChatPDU();
			pdu.setPduType(ChatPDU.CHAT_MESSAGE_RESPONSE);
			pdu.setServerThreadName(Thread.currentThread().getName());
			pdu.setClientThreadName(receivedPdu.getClientThreadName());
			pdu.setEventUserName(receivedPdu.getEventUserName());
			pdu.setUserName(receivedPdu.getEventUserName());
			pdu.setClientStatus(ChatClientConversationStatus.REGISTERED);
			ChatClientListEntry client = clients.getClient(receivedPdu
					.getUserName());
			pdu.setNumberOfSentEvents(eventCounter.longValue());

			if (client != null) {
				pdu.setClientStatus(client.getStatus());
				pdu.setServerTime(System.nanoTime() - client.getStartTime());
				pdu.setSequenceNumber(client.getNumberOfReceivedChatMessages());
				pdu.setNumberOfSentEvents(client.getNumberOfSentEvents());
				pdu.setNumberOfLostEventConfirms(client
						.getNumberOfLostEventConfirms());
				pdu.setNumberOfEventReceivedConfirms(client
						.getNumberOfReceivedEventConfirms());
				pdu.setNumberOfRetries(client.getNumberOfRetries());
				pdu.setNumberOfReceivedChatMessages(client
						.getNumberOfReceivedChatMessages());
			}
			return pdu;
		}

		/**
		 * Erzeugen einer Logout-Response-PDU
		 * 
		 * @param pdu
		 *            Empfangene PDU
		 * @return Erzeugte PDU
		 */
		private ChatPDU createLogoutResponsePdu(ChatPDU receivedPdu) {

			ChatPDU pdu = new ChatPDU();
			pdu.setPduType(ChatPDU.LOGOUT_RESPONSE);
			pdu.setServerThreadName(Thread.currentThread().getName());
			pdu.setClientThreadName(receivedPdu.getClientThreadName());
			pdu.setUserName(receivedPdu.getEventUserName());
			pdu.setClientStatus(ChatClientConversationStatus.UNREGISTERED);
			pdu.setNumberOfSentEvents(eventCounter.longValue());

			ChatClientListEntry client = clients.getClient(receivedPdu
					.getEventUserName());
			if (client != null) {
				pdu.setServerTime(System.nanoTime() - client.getStartTime());
				pdu.setClientStatus(client.getStatus());
				pdu.setNumberOfSentEvents(client.getNumberOfSentEvents());
				pdu.setNumberOfLostEventConfirms(client
						.getNumberOfLostEventConfirms());
				pdu.setNumberOfEventReceivedConfirms(client
						.getNumberOfReceivedEventConfirms());
				pdu.setNumberOfRetries(client.getNumberOfRetries());
				pdu.setNumberOfReceivedChatMessages(client
						.getNumberOfReceivedChatMessages());
			}
			return pdu;
		}

		/**
		 * Erzeugen einer Login-Response-PDU mit Fehlermeldung
		 * 
		 * @param pdu
		 *            Empfangene PDU
		 * @return Erzeugte PDU
		 */
		private ChatPDU createLoginErrorResponsePdu(ChatPDU receivedPdu,
				int errorCode) {

			ChatPDU pdu = new ChatPDU();
			pdu.setPduType(ChatPDU.LOGIN_RESPONSE);
			pdu.setServerThreadName(Thread.currentThread().getName());
			pdu.setClientThreadName(receivedPdu.getClientThreadName());
			pdu.setUserName(receivedPdu.getEventUserName());
			pdu.setClientStatus(ChatClientConversationStatus.UNREGISTERED);
			pdu.setErrorCode(errorCode);
			return pdu;
		}

		/**
		 * Senden eines Login-List-Update-Event an alle angemeldeten Clients
		 * 
		 * @param pdu
		 *            Zu sendende PDU
		 */
		private void sendLoginListUpdateEvent(ChatPDU pdu) {

			// Liste der eingeloggten User ermitteln
			Vector<String> clientList = clients.getClientNameList();

			// Beim Logout-Event den Client, der sich abmeldet, ausschließen
			if (pdu.getPduType() == ChatPDU.LOGOUT_EVENT) {
				clientList.remove(pdu.getEventUserName());
			}

			log.debug("Aktuelle Clientliste: " + clientList);

			pdu.setClients(clientList);

			for (String s : new Vector<String>(clientList)) {
				log.debug("Fuer "
						+ s
						+ " wird Login- oder Logout-Event-PDU an alle aktiven Clients gesendet");

				ChatClientListEntry client = clients.getClient(s);
				try {
					if (client != null) {

						client.getConnection().send(pdu);
						log.debug("Login- oder Logout-Event-PDU an "
								+ client.getUserName() + " gesendet");
					}
				} catch (Exception e) {
					log.debug("Senden einer Login- oder Logout-Event-PDU an "
							+ s + " nicht moeglich");
					ExceptionHandler.logException(e);
				}
			}

		}

		/**
		 * Login-Request bearbeiten: Neuen Client anlegen, alle Clients
		 * informieren
		 * 
		 * @param receivedPdu
		 *            Empfangene PDU
		 * @param con
		 *            Verbindung zum neuen Client
		 */
		private void login(ChatPDU receivedPdu, Connection con) {

			ChatPDU pdu;

			if (!clients.existsClient(receivedPdu.getUserName())) {

				log.debug("User nicht in Clientliste: "
						+ receivedPdu.getUserName());
				ChatClientListEntry client = new ChatClientListEntry(
						receivedPdu.getUserName(), con);
				client.setLoginTime(System.nanoTime());
				clients.createClient(receivedPdu.getUserName(), client);
				clients.changeClientStatus(receivedPdu.getUserName(),
						ChatClientConversationStatus.REGISTERING);
				log.debug("User " + receivedPdu.getUserName()
						+ " nun in Clientliste");
				userName = receivedPdu.getUserName();
				Thread.currentThread().setName(receivedPdu.getUserName());
				log.debug("Laenge der Clientliste: " + clients.size());

				clients.createWaitList(receivedPdu.getUserName());
				// Login-Event an alle Clients (auch an den gerade aktuell
				// anfragenden) senden

				pdu = createLoginEventPdu(receivedPdu);
				pdu.setMessage(clients.getWaitListSize(userName) + "");
				sendLoginListUpdateEvent(pdu);

			} else {
				// User bereits angemeldet, Fehlermeldung an Client senden,
				// Fehlercode an Client senden
				pdu = createLoginErrorResponsePdu(receivedPdu,
						ChatPDU.LOGIN_ERROR);

				try {
					con.send(pdu);
					log.debug("Login-Response-PDU an "
							+ receivedPdu.getUserName() + " mit Fehlercode "
							+ ChatPDU.LOGIN_ERROR + " gesendet");
				} catch (Exception e) {
					log.debug("Senden einer Login-Response-PDU an "
							+ receivedPdu.getUserName() + " nicth moeglich");
					ExceptionHandler.logExceptionAndTerminate(e);
				}
			}
		}

		/**
		 * Logout Request Bearbeiten, alle CLients Informieren
		 * 
		 * @param receivedPdu
		 *            Epfangende PDU
		 * @param con
		 *            Verbindung zm Client der den Logout benatragr
		 */
		private void logout(ChatPDU receivedPdu, Connection con) {
			ChatPDU pdu;
			if (clients.existsClient(receivedPdu.getUserName())) {

				clients.createWaitList(receivedPdu.getUserName());

				pdu = createLogoutEventPdu(receivedPdu);
				sendLoginListUpdateEvent(pdu);
				try {
					con.send(pdu);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				log.debug("User in Clientliste: " + receivedPdu.getUserName());
				clients.changeClientStatus(receivedPdu.getUserName(),
						ChatClientConversationStatus.UNREGISTERING);

				log.debug("User " + receivedPdu.getUserName()
						+ " nun nicht mehr in Clientliste");
				log.debug("Laenge der Clientliste: " + clients.size());
			}
		}

		/**
		 * Verbindung zu einem Client ordentlich abbauen
		 */
		private void closeConnection() {

			log.debug("Schliessen der Chat-Connection zum Client " + userName);

			// Bereinigen der Clientliste falls erforderlich

			if (clients.existsClient(userName)) {
				clients.finish(userName);
				log.debug("Close Connection fuer " + userName
						+ ", Laenge der Clientliste vor deleteClient: "
						+ clients.size());

				deleteClientWithoutCondition(userName);
				log.debug("Laenge der Clientliste nach deleteClient fuer: "
						+ userName + ": " + clients.size());
			}

			try {
				connection.close();
			} catch (Exception e) {
				ExceptionHandler.logException(e);
			}
		}

		/**
		 * Loescht einen Client zwangsweise inkl. aller Einträge in Wartelisten.
		 *
		 * @param userName
		 *            Name des Clients
		 */
		public synchronized void deleteClientWithoutCondition(String userName) {
			log.debug("Client  " + userName
					+ " zwangsweise aus allen Listen entfernen");
			for (String s : new HashSet<String>(clients.gcClientList())) {
				ChatClientListEntry client = (ChatClientListEntry) clients
						.getClient(s);
				if (client.getWaitList().contains(userName)) {
					client.getWaitList().remove(userName);
					// Hier kann es zu Fehler kommen, falls der client aus
					// dessen Waitlist der Client der sich ausloggt der letzte
					// ist , dies sollte mit dem Auskommentierten code behoben
					// werden war aber leider nicht erfolgreich.
					//
					// Die Idee für die Lösung dieses Problems finden sie hier:

					// if(client.getWaitList().size()==0)
					// {
					// ChatPDU pdu = new ChatPDU();
					//
					//
					// pdu.setUserName(client.getUserName());
					// pdu.setEventUserName(client.getUserName());
					//
					// System.out.println("xy");
					// switch (client.getStatus()) {
					// case UNREGISTERING:
					// pdu.setPduType(ChatPDU.LOGOUT_EVENT_CONFIRM);
					// break;
					// case REGISTERING:
					// pdu.setPduType(ChatPDU.LOGIN_EVENT_CONFIRM);
					// break;
					// case REGISTERED:
					// pdu.setPduType(ChatPDU.CHAT_MESSAGE_EVENT_CONFIRM);
					// break;
					// default:
					// break;
					// }
					// try {
					// clients.getClient(client.getUserName()).getConnection().send(pdu);
					// } catch (Exception e) {
					// // TODO Auto-generated catch block
					// e.printStackTrace();
					// }
					// }
				}
			}

			// Client kann nun entfernt werden
			clients.deleteClient(userName);
			log.debug("Client  " + userName
					+ " vollstaendig aus allen Wartelisten entfernt");
		}

		/**
		 * Verarbeitung einer ankommenden Nachricht eines Clients
		 * (Zustandsautomat des Servers)
		 * 
		 * @throws Exception
		 */
		private void handleIncomingMessage() throws Exception {

			// Warten auf naechste Nachricht

			ChatPDU receivedPdu;
			try {

				receivedPdu = (ChatPDU) connection.receive();
				userName = receivedPdu.getUserName();
				startTime = System.nanoTime(); // Zeitmessung fuer RTT starten
			} catch (Exception e) {
				log.error("Empfang einer Nachricht fehlgeschlagen, Workerthread fuer User: "
						+ userName);
				finished = true;
				ExceptionHandler.logException(e);
				return;
			}

			// Empfangene Nachricht bearbeiten
			try {
				switch (receivedPdu.getPduType()) {

				case ChatPDU.LOGIN_REQUEST:
					// Neuer Client moechte sich einloggen, Client in
					// Client-Liste eintragen
					log.debug("Login-Request-PDU fuer "
							+ receivedPdu.getUserName() + " empfangen");
					System.out.println("sie sind im login request" + "||||"
							+ "        " + receivedPdu.getEventUserName());
					login(receivedPdu, connection);

					break;

				case ChatPDU.LOGOUT_REQUEST:
					// Hier wird ein Logout-Request von einem Client behandelt
					log.debug("Logout-Request-PDU fuer "
							+ receivedPdu.getUserName() + " empfangen");
					System.out.println("sie sind im logout request" + "||||"
							+ "        " + receivedPdu.getEventUserName());
					logout(receivedPdu, connection);

					break;

				case ChatPDU.CHAT_MESSAGE_REQUEST:
					// Hier wird eine Chat-Message-Request von einem Client
					// bearbeitet
					System.out.println("sie sind im message request" + "||||"
							+ "        " + receivedPdu.getUserName());
					clients.createWaitList(receivedPdu.getUserName());
					try {
						clients.deleteWaitListEntry(receivedPdu.getUserName(),
								receivedPdu.getUserName());
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					ChatPDU pdu;

					pdu = createChatMessageEventPdu(receivedPdu);
					sendMEssageUpdatePdu(pdu);

					break;

				case ChatPDU.CHAT_MESSAGE_EVENT_CONFIRM:
					// Hier wird die Chat-Message-Event-Confirm vom Server
					// bearbeitet,
					// aus der Warteliste des Clients, der für die
					// korrespondirende Chat-Message-Request-Pdu verantwortlich
					// ist ,
					// wird der client gelöscht, welcher den confirm gesendet
					// hat,
					// sollte die Warteliste danch leer sien wird eine
					// Chat-Message-Response PDU gesendet
					clients.deleteWaitListEntry(receivedPdu.getEventUserName(),
							userName);

					clients.getClient(receivedPdu.getEventUserName())
							.setNumberOfReceivedEventConfirms(
									clients.getClient(
											receivedPdu.getEventUserName())
											.getNumberOfReceivedEventConfirms() + 1);

					System.out.println("sie sind im message confirm" + "||||"
							+ "        " + receivedPdu.getEventUserName());
					System.out.println(clients.getWaitListSize(receivedPdu
							.getEventUserName()));
					if (clients.getWaitListSize(receivedPdu.getEventUserName()) == 0) {
						clients.deleteWaitList(receivedPdu.getEventUserName());
						pdu = createChatMessageResponsePdu(receivedPdu);

						try {
							if (clients.getClient(receivedPdu
									.getEventUserName()) != null) {
								clients.getClient(
										receivedPdu.getEventUserName())
										.getConnection().send(pdu);
								log.debug("Chat-Message-Response-PDU an "
										+ receivedPdu.getEventUserName()
										+ " gesendet");
							}
						} catch (Exception e) {
							log.error("Chat-Message-Response-PDU an "
									+ receivedPdu.getEventUserName()
									+ " nicht moeglich");
							ExceptionHandler.logException(e);
						}
					}
					break;

				case ChatPDU.LOGIN_EVENT_CONFIRM:
					// Hier wird die Login Event Confirm vom Server bearbeitet,
					// aus der Warteliste des Clients, der für die
					// korrespondirende Login-Request-Pdu verantwortlich ist ,
					// wird der client gelöscht, welcher den confirm gesendet
					// hat,
					// sollte die Warteliste danch leer sien wird eine
					// Login-Response PDU gesendet
					clients.deleteWaitListEntry(receivedPdu.getEventUserName(),
							userName);
					clients.getClient(receivedPdu.getEventUserName())
							.setNumberOfReceivedEventConfirms(
									clients.getClient(
											receivedPdu.getEventUserName())
											.getNumberOfReceivedEventConfirms() + 1);

					System.out.println("sie sind im login confirm" + "||||"
							+ "        " + receivedPdu.getEventUserName());
					System.out.println(clients.getWaitListSize(receivedPdu
							.getEventUserName()));
					if (clients.getWaitListSize(receivedPdu.getEventUserName()) == 0) {
						clients.deleteWaitList(receivedPdu.getEventUserName());
						pdu = createLoginResponsePdu(receivedPdu);

						try {
							if (clients.getClient(receivedPdu
									.getEventUserName()) != null) {
								clients.getClient(
										receivedPdu.getEventUserName())
										.getConnection().send(pdu);
								log.debug("Login-Response-PDU an "
										+ receivedPdu.getEventUserName()
										+ " gesendet");
							}
						} catch (Exception e) {
							log.error("Senden einer Login-Response-PDU an "
									+ receivedPdu.getEventUserName()
									+ " nicht moeglich");
							ExceptionHandler.logException(e);
						}

						clients.changeClientStatus(
								receivedPdu.getEventUserName(),
								ChatClientConversationStatus.REGISTERED);

					}

					break;

				case ChatPDU.LOGOUT_EVENT_CONFIRM:
					// Hier wird die Logout-Event-Confirm vom Server bearbeitet,
					// aus der Warteliste des Clients, der für die
					// korrespondirende Logout-Request-Pdu verantwortlich ist ,
					// wird der client gelöscht, welcher den confirm gesendet
					// hat,
					// sollte die Warteliste danch leer sien wird eine
					// Logout-Response PDU gesendet
					clients.deleteWaitListEntry(receivedPdu.getEventUserName(),
							userName);

					clients.getClient(receivedPdu.getEventUserName())
							.setNumberOfReceivedEventConfirms(
									clients.getClient(
											receivedPdu.getEventUserName())
											.getNumberOfReceivedEventConfirms() + 1);

					System.out.println("sie sind im logout cpnfirm" + "||||"
							+ "        " + receivedPdu.getEventUserName());
					System.out.println(clients.getWaitListSize(receivedPdu
							.getEventUserName()));
					if (clients.getWaitListSize(receivedPdu.getEventUserName()) == 0) {
						clients.deleteWaitList(receivedPdu.getEventUserName());

						pdu = createLogoutResponsePdu(receivedPdu);

						try {
							if (clients.getClient(receivedPdu
									.getEventUserName()) != null) {
								clients.getClient(
										receivedPdu.getEventUserName())
										.getConnection().send(pdu);
								log.debug("Logout-Response-PDU an "
										+ receivedPdu.getEventUserName()
										+ " gesendet");
							}
						} catch (Exception e) {
							log.error("Senden einer Logout-Response-PDU an "
									+ receivedPdu.getEventUserName()
									+ " nicht moeglich");
							ExceptionHandler.logException(e);
						}

						clients.changeClientStatus(
								receivedPdu.getEventUserName(),
								ChatClientConversationStatus.UNREGISTERED);

						// Hier gibt es das Problem, die Clients ordentlich aus
						// der SharedClientList zu Löschen
						// Die Überprüfung ob der Client gelöscht werden darf
						// macht hierbei probleme

						logoutCounter.incrementAndGet();
						// this.finished=true;
						// closeConnection();
						if (clients
								.deleteClient(receivedPdu.getEventUserName()))
							;// {
								// this.finished = true;}
						// nachdem wir den ChatWorker nicht einfach auf finished
						// setzen können und der client noch in der liste steht
						// haben wir probleme
						// da dieser bei neuen request noch benachrichtigt wird,
						// dieser aber nicht mehr antwortet
						// Das Problem wäre einfach gelöst, wenn der Client
						// gelöscht und der Chatworker auf finished gesetzt
						// werden könnte

						// Ein ALternativer Ansatz zu diesem Porblem (als der
						// der in deleteClietWithoutConditen() beschrieben ist,
						// wäre: Am Ende jeder Logout-Event-Confirm zu schauen
						// ob der Client gelöscht werden darf, wenn dies nicht
						// der Fall ist wird keine PDU gesendet.
						// Sobald ein Client gelöscht werden darf, geht man mit
						// einer Schleife über alle clients welche im Staus
						// Unregistering sind und sendet ihnen die
						// Logout-event-confirm PDU
						// (zur bestimmung ob ein Client gelöscht werden darf
						// oder nicht würde die vordefinierte Methode
						// clients.deleteClient() verwendet werden.

					}
					break;

				default:
					log.debug("Falsche PDU empfangen von Client: "
							+ receivedPdu.getUserName() + ", PduType: "
							+ receivedPdu.getPduType());
					break;
				}
			} catch (Exception e) {
				log.error("Exception bei der Nachrichtenverarbeitung");
				ExceptionHandler.logExceptionAndTerminate(e);
			}

		}

		/**
		 * Hier wird eine Nachricht an alle aktive clients gesendet
		 * 
		 * @param pdu
		 *            die Nachricht die vertailt werden soll
		 */
		private void sendMEssageUpdatePdu(ChatPDU pdu) {

			// Liste der eingeloggten User ermitteln
			Vector<String> clientList = clients.getClientNameList();

			// Beim Logout-Event den Client, der sich abmeldet, ausschließen

			// log.debug("Aktuelle Clientliste: " + clientList);

			pdu.setClients(clientList);

			for (String s : new Vector<String>(clientList)) {
				// log.debug("Fuer " + s +
				// " wird Login- oder Logout-Event-PDU an alle aktiven Clients gesendet"
				// );

				ChatClientListEntry client = clients.getClient(s);
				client.setNumberOfReceivedChatMessages(client
						.getNumberOfReceivedChatMessages() + 1);
				try {
					if (client != null) {

						client.getConnection().send(pdu);
						// log.debug("Login- oder Logout-Event-PDU an " +
						// client.getUserName() + " gesendet");
					}
				} catch (Exception e) {
					// log.debug("Senden einer Login- oder Logout-Event-PDU an "
					// + s + " nicht moeglich");
					ExceptionHandler.logException(e);
				}
			}
		}
	}
}
