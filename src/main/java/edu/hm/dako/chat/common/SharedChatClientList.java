package edu.hm.dako.chat.common;

import java.util.HashSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Liste aller angemeldeten Clients. Diese Liste wird im Server als Singleton verwaltet
 * (darf nur einmal erzeugt werden). Alle Worker-Threads im Server nutzen diese Liste.
 *
 * Die Liste wird als HashMap organisiert. Als Schl√ºssel wird der Username von Clients verwendet.
 *
 * Genereller Hinweis: Zur Umgehung von ConcurrentModificationExceptions wird bei der Iteration durch
 * Listen generell eine Kopie der Liste angelegt.
 *
 * @author mandl
 *
 */
public class SharedChatClientList {
	
     private static Log log = LogFactory.getLog(SharedChatClientList.class);	
	 private static ConcurrentHashMap<String, ChatClientListEntry> clients;  // Liste aller eingeloggten Clients	
	 //private static HashMap<String, ChatClientListEntry> clients;  // Liste aller eingeloggten Clients		
	 private static SharedChatClientList instance;
	    
	 private SharedChatClientList() {}
	
	 /**
	  * Thread-sicheres Erzeugen einer Instanz der Liste
	  * @return Referenz auf die erzeugte Liste
	  */
	 public static synchronized SharedChatClientList getInstance () {
	    if (SharedChatClientList.instance == null) {
	         SharedChatClientList.instance = new SharedChatClientList();
	  		 // Clientliste nur einmal erzeugen
	         clients = new ConcurrentHashMap<String, ChatClientListEntry>();
	    }
	    return SharedChatClientList.instance;	        
	}

	
	/**
	 * Loeschen der gesamten Liste
	 */	
	public void deleteAll() {
		 clients.clear();
	}
	
	/**
	 * Status eines Clients veraendern
	 * 
	 * @param userName Name des Users (Clients)
	 * @param newStatus Neuer Status 
	 */
	public synchronized void changeClientStatus(String userName, ChatClientConversationStatus newStatus) {
		
        ChatClientListEntry client = clients.get(userName);
      	client.setStatus(newStatus);
        clients.replace(userName, client);
        log.debug("User " + userName + " nun in Status: " + newStatus);
    }
	
	/**
	 * Lesen des Conversation-Status fuer einen Client
	 * 
	 * @param userName Name des Users (Clients)
	 * @return Conversation-Status des Clients
	 */
	public synchronized ChatClientConversationStatus getClientStatus(String userName) {
		
		ChatClientListEntry client = clients.get(userName);
		if (client != null) {
			return (client.getStatus());
		} else {
			return ChatClientConversationStatus.UNREGISTERED;
		}	
	}
	
	/**
	 * Client auslesen
	 * 
	 * @param userName Name des Clients
	 * @return Referenz auf den gesuchten Client
	 */
	public synchronized ChatClientListEntry getClient(String userName) {
		
		return clients.get(userName);
	}
	
	/**
	 * Stellt eine Liste aller Namen der eingetragenen Clients bereit
	 * 
	 * @return Vektor mit allen Namen der eingetragenen Clients
	 */
	public synchronized Vector<String> getClientNameList() {		

		Vector<String> clientNameList = new Vector<String>();
		for (String s : new HashSet<String>(clients.keySet())) {
    		  clientNameList.add(s);
        }
		return clientNameList;
	}

	/**
	 * Prueft, ob ein Client in der Userliste ist
	 * 
	 * @param userName Name des Clients
	 * @return
	 */
	public synchronized boolean existsClient(String userName) {
	
		if (userName != null) {
			if (! clients.containsKey(userName)) {
				log.debug("User nicht in Clientliste: " + userName);
				return false;
			} else {
				return true;
			}
		} else {
			return false;
		}
	}
    	 
	/**
	 * Legt einen neuen Client an
	 * 
	 * @param userName Name des neuen Clients
	 * @param client Client-Daten
	 */
	public synchronized void createClient(String userName, ChatClientListEntry client) {
		
		//clients.putIfAbsent(userName, client);
		clients.put(userName, client);
	}
	  
	/**
	 * Aktualisierung eines vorhandenen Clients
	 * 
	 * @param userName Name des Clients
	 * @param client Client-Daten
	 */
	public synchronized void updateClient(String userName, ChatClientListEntry client) {
		
		ChatClientListEntry existingClient = (ChatClientListEntry) clients.get(userName);
		
		if ( existingClient != null) {
			 clients.put(userName, client);
		} else {
			log.debug("User nicht in Clientliste: " + userName);
		}
	}
   
	/**
	 *
	 * Entfernt einen Client aus der Clientliste.
	 * Der Client darf nur geloescht werden, wenn er nicht mehr in der Warteliste eines anderen Client ist.
	 * 
	 * @param userName Name des Clients
	 * @return true bei erfolgreichem Loeschen, sonst false
	 */
	
	public synchronized boolean deleteClient(String userName) {

		log.debug("Clientliste vor dem Loeschen von " + userName + ": " + printClientList());
  	  	log.debug("Logout fuer " + userName + ", Laenge der Clientliste vor deleteClient: " + clients.size());
  	   
		boolean deletedFlag = false;
		ChatClientListEntry removeCandidateClient = (ChatClientListEntry) clients.get(userName);
		if ( removeCandidateClient != null) {

			// Event-Warteliste des Clients leer?
			log.debug("Laenge der Clientliste " + userName + ": " + clients.size());
			if ( (removeCandidateClient.getWaitList().size() == 0) && (removeCandidateClient.isFinished()) ) {

				// Warteliste leer, jetzt pruefen, ob er noch in anderen Wartelisten ist
				log.debug("Warteliste von Client " + removeCandidateClient.getUserName() +
						" ist leer und Client ist zum Beenden vorgemerkt");

				for (String s : new HashSet<String>(clients.keySet())) {		
					ChatClientListEntry client = (ChatClientListEntry) clients.get(s);
					if (client.getWaitList().size()!= 0){
						log.debug("Loeschen nicht moeglich, da Client " + s + " noch in der Warteliste von " +
								client.getUserName() + " ist" );
						return deletedFlag;
					}
				}

				// Client kann entfernt werden, sofern er auch zum Beenden vorgemerkt ist.
				clients.remove(userName);
				deletedFlag = true;
			}
		}
		
		log.debug("Logout: Laenge der Clientliste nach deleteClient fuer: " + userName + ": " + clients.size());  
		log.debug("Clientliste nach dem Loeschen von " + userName + ": " + printClientList());
		return deletedFlag;
	}

	/**
	 * Garbage Collector der Clientliste bereinigt nicht mehr benoetigte Clients
	 *
	 * @return Namensliste aller entfernten Clients
	 */
	public synchronized Vector<String> gcClientList() {

		Vector<String> deletedClients = new Vector<String>();

		for (String s1 : new Vector<String>(clients.keySet())) {
			boolean clientUsed = true;
			ChatClientListEntry client1 = (ChatClientListEntry) clients.get(s1);
			if ( (client1.getWaitList().size() == 0) && (client1.isFinished()) ) {

				// Eigene Warteliste leer, jetzt pruefen, ob auch alle anderen Wartelisten
				// diesen Client nicht enthalten
				clientUsed = false;
				for (String s2 : new Vector<String>(clients.keySet())) {
					ChatClientListEntry client2 = (ChatClientListEntry) clients.get(s2);
					if (client2.getWaitList().contains(s1)) {
						// Client noch in einer Warteliste
						clientUsed = true;
					}
				}
			}
			if (!clientUsed) {
				log.debug("Garbace Collection: Client " + client1.getUserName() + " wird aus ClientListe entfernt" );
				deletedClients.add(s1);
				clients.remove(s1);
			}
		}
		return deletedClients;
	}
   
	/**
	 * Laenge der Liste ausgeben
	 * 
	 * @return Laenge der Liste
	 */
	public synchronized long size() {
		
	   return clients.size();
	}
	
	/**
	 * Erhoeht den Zaehler fuer empfangene Chat-Event-Confirm-PDUs fuer einen Client
	 * 
	 * @param userName Name des Clients
	 */
	public synchronized void incrNumberOfReceivedChatEventConfirms(String userName) {
		
		 ChatClientListEntry client = clients.get(userName);
		 if ( client != null) {
			 client.incrNumberOfReceivedEventConfirms();
		 }
	}
	
	/**
	 * Erhoeht den Zaehler fuer gesendete Chat-Event-PDUs fuer einen Client
	 * 
	 * @param userName Name des Clients
	 */
	public synchronized void incrNumberOfSentChatEvents(String userName) {
		
		 ChatClientListEntry client = clients.get(userName);
		 if ( client != null) {
			 client.incrNumberOfSentEvents();
		 }
	}
	
	/**
	 * Erhoeht den Zaehler fuer empfangene Chat-Message-PDUs eines Client
	 * 
	 * @param userName Name des Clients
	 */
	public synchronized void incrNumberOfReceivedChatMessages(String userName) {
		
		 ChatClientListEntry client = clients.get(userName);
		 if ( client != null) {
			 client.incrNumberOfReceivedChatMessages();
		 }
	}
	
	/**
	 * Setzt die Ankunftszeit eines Chat-Requests fuer die Serverzeitmessung
	 * 
	 * @param userName Name des Clients
	 */
	
	public synchronized void setRequestStartTime(String userName, long startTime) {
		
		ChatClientListEntry client = clients.get(userName);
	 	if (client != null) {
		   client.setStartTime(startTime);
	 	}
	}
	
	/**
	 * Liefert die Ankunftszeit eines Chat-Requests  
	 * 
	 * @param userName  Name des Clients
	 * @return Ankunftszeit des Requests in ns
	 */	
	public synchronized long getRequestStartTime(String userName) {
		
		ChatClientListEntry client = clients.get(userName);
	 	if ( client != null) {
	 		return (client.getStartTime());
	 	}
	 	else {
	 		return 0;
	 	}
	}

	/**
	 * Erstellt eine Liste aller Clients, die noch ein Event bestaetigen muessen
	 * 
	 * @param userName Name des Clients, fuer den die Liste erstellt werden soll
	 */
	public synchronized void createWaitList(String userName) {
	
		 ChatClientListEntry client = clients.get(userName);
		 if (client != null) {
			 for (String s : new HashSet<String> (clients.keySet())) {
				 client.addWaitListEntry(s);
	        }
			log.debug("Warteliste fuer " + userName + " erzeugt");
		 } else {
				log.debug("Warteliste fuer " + userName + " konnte nicht erzeugt werden");
		 }
	}
	
	/**
	 * Loescht eine Event-Warteliste fuer einen Client
	 * 
	 * @param userName Name des Clients, fuer den die Liste geloesccht werden soll
	 */
	public synchronized void deleteWaitList(String userName) {
		
		 ChatClientListEntry client = clients.get(userName);
		 if ( client != null) {
			 client.clearWaitList();
		 }
	}	
	
	/**
	 * Loescht einen Eintrag aus der Event-Warteliste
	 * 
	 * @param userName Name des Clients, fuer den ein Listeneintrag geloescht werden soll
	 * @param entryName name des Clients, der aus der Event-Warteliste geloescht werden soll
	 * @return Anzahl der noch vorhandenen Eintraege in der Liste
	 */
	public synchronized void deleteWaitListEntry(String userName, String entryName) throws Exception {

		log.debug("Client: " + userName + ", aus Warteliste von " + entryName + " loeschen ");
		 
		 ChatClientListEntry client = clients.get(userName);
		 
		 if (client == null) {
			log.debug("Kein Eintrag fuer " + userName + " in der Clientliste vorhanden");
			Exception e = new Exception();
			throw new Exception(e);
		 } else if (client.getWaitList().size() == 0) {
			log.debug("Warteliste fuer " + userName + " war vorher schon leer");
	        return;	
		 } else {
			 client.getWaitList().remove(entryName);
			 log.debug("Eintrag fuer " + entryName + " aus der Warteliste von " + client.getUserName() + " geloescht");
		 }
	}
	
	/**
	 * Liefert die Laenge der Event-Warteliste fuer einen Client
	 * 
	 * @param userName Name des Clients
	 * @return Anzahl der noch vorhandenen Eintraege in der Liste
	 */
	public synchronized int getWaitListSize(String userName) {
		
		 ChatClientListEntry client = clients.get(userName);		 
		 if (client != null) {
			 	return client.getWaitList().size();
		 }
	     return 0;
	}	
	
	/**
	 * Setzt Kennzeichen, das die Arbeit fuer einen User eingestellt werden kann
	 * 
	 * @param userName Name des Clients 
	 */
	public synchronized void finish(String userName) {

		ChatClientListEntry client = clients.get(userName);		 
		 if (client != null) {
			 client.setFinished(true);
			 log.debug("Finished-Kennzeichen gesetzt fuer: " + userName);
		 }
	}

	/**
	 * Ausgeben der aktuellen Clientliste einschliesslich der Wartelisten der Clients
	 */
	public String printClientList() {

		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("Clientliste mit zugehoerigen Wartelisten: ");

		if (clients.isEmpty()) {
			stringBuilder.append(" leer\n");
		} else {
			stringBuilder.append("\n");
			for (String s : new HashSet<String> (clients.keySet())) {
				ChatClientListEntry client = clients.get(s);
				stringBuilder.append(client.getUserName() + ", ");
				stringBuilder.append(client.getWaitList() + "\n");
			}
		}
		return stringBuilder.toString();
	}
	
	/**
	 * Loescht einen Client zwangsweise inkl. aller Eintr‰ge in Wartelisten.
	 *
	 * @param userName Name des Clients
	 */
	public synchronized void deleteClientWithoutCondition(String userName) {
	    log.debug("Client  " + userName + " zwangsweise aus allen Listen entfernen");
	    for (String s : new HashSet<String>(clients.keySet())) {
	        ChatClientListEntry client = (ChatClientListEntry) clients.get(s);
	        if (client.getWaitList().contains(userName)) {
	            client.getWaitList().remove(userName);
	        }
	    }

	    // Client kann nun entfernt werden
	    clients.remove(userName);
	    log.debug("Client  " + userName + " vollstaendig aus allen Wartelisten entfernt");
	}
}
