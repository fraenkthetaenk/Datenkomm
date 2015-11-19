package edu.hm.dako.chat.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.concurrent.CountDownLatch;

/**
 * Die Klasse sammelt Statistikdaten zur Ermittlung von Round Trip Times (RTT) fuer einen
 * Test zur Kommunikation zwischen mehreren Client-Threads und einem Server.
 * <p/>
 * Die Daten werden in einem Array gesammelt, das einen Eintrag fuer jeden Client
 * enthaelt. Jeder Client erhaelt eine Nummer, die als Zugriffsindex auf das Array verwendet
 * wird.
 *
 * @author Mandl
 */
public class SharedClientStatistics {
    private static Log log = LogFactory.getLog(SharedClientStatistics.class);

    // Anzahl von Clients
    private int numberOfClients;

    // Anzahl der Nachrichten (Requests) eines Clients
    private int numberOfMessages;

    // Denkzeit eines Clients zwischen zwei Requests in ms
    private int clientThinkTime;

    // Alle Antwortnachrichten, die fuer den Test empfangen werden muessen
    private int numberOfAllMessages;
    
    // Alle Event-Nachrichten, die fuer den Test vom Server gesendet werden muessen
    long numberOfPlannedEventMessages;

    // Zaehlt angemeldete Clients
    private int numberOfLoggedInClients;
   
    // Kann benutzt werden um ein gleichzeitiges Starten aller Client-Threads zu ermoeglichen
    private CountDownLatch loginSignal;
    
    // Kann benutzt werden um ein gleichzeitiges Logout aller Client-Threads zu ermoeglichen, erst nachdem alle
    // Chat-Messages von allen Clients versendet wurden
    private CountDownLatch logoutSignal;

    // Statistikdaten eines Clients
    private class ClientStatistics {
        int sentRequests;         			// Anzahl gesendeter Nachrichten
        int receivedResponses;    			// Anzahl empfangener Antworten
        int numberOfRetries;	  			// Anzahl an Uebertragungswiederholungen (fuer unzuverlaessige Verbindungen wie UDP)
        long numberOfSentEventMessages; 	// Anzahl gesendeter Events fuer den Client
        long numberOfReceivedConfirmEvents;	// Anzahl empfangener Responses fuer den Client
        long numberOfLostConfirmEvents; 	// Anzahl verlorender Event-Bestaetigungen fuer den Client
        long numberOfRetriedEvents; 		// Anzahl von wiederholten Events fuer den Client (fuer unzuverlaessige Verbindungen wie UDP)
        long averageRTT;          			// Durchschnittliche Round Trip Time in ns
        long sumRTT;              			// Summe aller RTTs in ns
        long sumServerTime;       			// Zeit, die der Server insgesamt fuer alle Requests benoetigt in ns
        long maxHeapSize;         			// Maximale Heap-Groesse in Bytes waehrend eines Testlaufs
    }

    private ClientStatistics clientStatistics[];

    /**
     * Test, ob Client-Id im gueltigen Bereich ist
     *
     * @param i Client-Id
     * @return true, falls Client-Id im gueltigen Bereich ist. Sonst false.
     */
    private boolean inRange(int i) {
        if ((i < 0) || (i > numberOfClients)) {
            log.error("Client-Id nicht im gueltigen Bereich");
            return false;
        } else {
            return true;
        }
    }

    public SharedClientStatistics(int numberOfClients, int numberOfMessages, int clientThinkTime) {
        this.numberOfClients = numberOfClients;
        this.numberOfMessages = numberOfMessages;
        this.clientThinkTime = clientThinkTime;
        this.numberOfAllMessages = numberOfClients * numberOfMessages;
        this.numberOfPlannedEventMessages = numberOfAllMessages * numberOfClients;
        loginSignal = new CountDownLatch(numberOfClients);
        logoutSignal = new CountDownLatch(numberOfClients);
        clientStatistics = new ClientStatistics[numberOfClients];

        // Initialisieren der Statistik-Tabelle
        for (int i = 0; i < numberOfClients; i++) {
            clientStatistics[i] = new ClientStatistics();
            clientStatistics[i].receivedResponses = 0;
            clientStatistics[i].sentRequests = 0;
            clientStatistics[i].numberOfRetries = 0;
            clientStatistics[i].numberOfSentEventMessages = 0;
            clientStatistics[i].numberOfReceivedConfirmEvents = 0;	
            clientStatistics[i].numberOfLostConfirmEvents = 0; 	
            clientStatistics[i].numberOfRetriedEvents = 0; 	
            clientStatistics[i].averageRTT = 0;
            clientStatistics[i].sumRTT = 0;
            clientStatistics[i].sumServerTime = 0;
            clientStatistics[i].maxHeapSize = 0;
        }
    }

    public CountDownLatch getLoginSignal() {
        return loginSignal;
    }
    
    public CountDownLatch getLogoutSignal() {
        return logoutSignal;
    }

    /**
     * Anzahl der angemeldeten Clients erhoehen
     */
    public synchronized void incrNumberOfLoggedInClients() {
        numberOfLoggedInClients++;
        if (numberOfLoggedInClients == numberOfClients) {
            log.debug("Alle " + numberOfClients + " Test-Clients angemeldet");
        }
    }

    /**
     * Anzahl der gesendeten Nachrichten eines Clients erhoehen
     *
     * @param i Client-Id
     */
    public synchronized void incrSentMsgCounter(int i) {
        if (!inRange(i)) return;
        clientStatistics[i].sentRequests++;
    }

    /**
     * Anzahl der gesendeten Events setzen
     */
    public synchronized void setNumberOfSentEventMessages(int i, long nr) {
        if (!inRange(i)) return;
        clientStatistics[i].numberOfSentEventMessages = nr;
    }
    
    /**
     * Anzahl der gesendeten Events lesen
     */
    public synchronized long getNumberOfSentEventMessages(int i) {
        if (!inRange(i)) return (-1);
        return clientStatistics[i].numberOfSentEventMessages;
    }    
    
    /**
     * Anzahl der verlorenen Event-Bestaetigungen setzen
     */
    public synchronized void setNumberOfLostConfirmEvents(int i, long nr) {
        if (!inRange(i)) return;
        clientStatistics[i].numberOfLostConfirmEvents = nr;
    }
    
    
    /**
     * Anzahl der verlorenen Confirms lesen
     */
    public synchronized long getNumberOfLostConfirmEvents(int i) {
        if (!inRange(i)) return (-1);
        return clientStatistics[i].numberOfLostConfirmEvents;
    }
   
    /**
     * Anzahl der empfangenen Event-Bestaetigungen setzen
     */
    public synchronized void setNumberOfReceivedConfirmEvents(int i, long nr) {
        if (!inRange(i)) return;
        clientStatistics[i].numberOfReceivedConfirmEvents = nr;
    }
    
    /**
     * Anzahl der empfangenen Confirms lesen
     */
    public synchronized long getNumberOfReceivedConfirmEvents(int i) {
        if (!inRange(i)) return (-1);
        return clientStatistics[i].numberOfReceivedConfirmEvents;
    }
    
    /**
     * Anzahl der Event-Wiederholungen setzen
     */
    public synchronized void setNumberOfRetriedEvents(int i, long nr) {
        if (!inRange(i)) return;
        clientStatistics[i].numberOfRetriedEvents = nr;
    }
    
   /**
    * Anzahl der wiederholten Confirms lesen
    */
    public synchronized long getNumberOfRetriedEvents(int i) {
       if (!inRange(i)) return (-1);
       return clientStatistics[i].numberOfRetriedEvents;
    }
 
    /**
     * Anzahl der Uebrtragungswiederholungen erhoehen
     */
    public synchronized void incrNumberOfRetries(int i) {
        if (!inRange(i)) return;
        clientStatistics[i].numberOfRetries++;
    }
    /**
     * Anzahl der empfangenen Nachrichten eines Clients erhoehen
     *
     * @param i          Client-Id
     * @param rtt        RoundTrip Time
     * @param serverTime Die Zeit, die der Server benötigt hat
     */
    public synchronized void incrReceivedMsgCounter(int i, long rtt, long serverTime) {
        if (!inRange(i)) return;
        clientStatistics[i].receivedResponses++;
        clientStatistics[i].sumRTT = clientStatistics[i].sumRTT + rtt;
        clientStatistics[i].averageRTT = clientStatistics[i].sumRTT / clientStatistics[i].receivedResponses;
        clientStatistics[i].sumServerTime = clientStatistics[i].sumServerTime + serverTime;
        if (clientStatistics[i].maxHeapSize < usedMemory()) {
            clientStatistics[i].maxHeapSize = usedMemory();
        }
    }

    /**
     * Test, ob alle Response-Nachrichten empfangen wurden
     *
     * @return true Alle erwarteten Nachrichten empfangen;
     *         false Noch nicht alle erwarteten Nachrichten empfangen
     */
    public synchronized boolean allMessagesReceived() {
        int sum = 0;
        for (int i = 0; i < numberOfClients; i++) {
            sum += clientStatistics[i].receivedResponses;
        }
        return sum == numberOfAllMessages;
    }

    /**
     * Test, ob alle Clients angemeldet sind
     *
     * @return true angemeldet;
     *         false nicht angemeldet
     */
    public synchronized boolean allClientsLoggedIn() {
        return numberOfLoggedInClients == numberOfClients;
    }

    /**
     * Test, ob alle Nachrichten eines Clients angekommen sind
     *
     * @param i: Client-Nummer
     * @return true Alle angekommen;
     *         false Nicht alle angekommen
     */
    public synchronized boolean allMessageReceived(int i) {
        if (!inRange(i)) return (false);
        return clientStatistics[i].receivedResponses == numberOfMessages;
    }

    /**
     * Anzahl aller empfangenen Event-Confirm-Nachrichten ermitteln
     *
     * @return Anzahl empfangener Event-Confirm-Nachrichten
     */
    public synchronized int getSumOfAllReceivedConfirmEvents() {
        int sum = 0;
        for (int i = 0; i < numberOfClients; i++) {
            sum += clientStatistics[i].numberOfReceivedConfirmEvents;
        }
        return sum;
    }
    
    /**
     * Anzahl aller verlorenen Event-Confirm-Nachrichten ermitteln
     *
     * @return Anzahl verlorener Event-Confirm-Nachrichten
     */
    public synchronized int getSumOfAllLostConfirmEvents() {
        int sum = 0;
        for (int i = 0; i < numberOfClients; i++) {
            sum += clientStatistics[i].numberOfLostConfirmEvents;
        }
        return sum;
    }
    
    /**
     * Anzahl aller Wiederholungen von Event-Nachrichten ermitteln
     *
     * @return Anzahl verlorener Event-Nachrichten
     */
    public synchronized int getSumOfAllRetriedEvents() {
        int sum = 0;
        for (int i = 0; i < numberOfClients; i++) {
            sum += clientStatistics[i].numberOfRetriedEvents;
        }
        return sum;
    }
    
    /**
     * Anzahl aller gesendeten Event-Nachrichten ermitteln
     *
     * @return Anzahl gesendeten Event-Nachrichten
     */
    public synchronized int getSumOfAllSentEventMessages() {
        int sum = 0;
        for (int i = 0; i < numberOfClients; i++) {
            sum += clientStatistics[i].numberOfSentEventMessages;
        }
        return sum;
    }
    
    /**
     * Anzahl aller empfangenen Nachrichten ermitteln
     *
     * @return Anzahl empfangener Nachrichten
     */
    public synchronized int getSumOfAllReceivedMessages() {
        int sum = 0;
        for (int i = 0; i < numberOfClients; i++) {
            sum += clientStatistics[i].receivedResponses;
        }
        return sum;
    }
    
    /**
     * Anzahl aller Uebertragungswiederholungen ermitteln
     *
     * @return Anzahl Uebertragungswiederholungen
     */
    public synchronized int getSumOfAllRetries() {
        int sum = 0;
        for (int i = 0; i < numberOfClients; i++) {
            sum += clientStatistics[i].numberOfRetries;
        }
        return sum;
    }

    /**
     * Durchschnittliche RTT ermitteln
     *
     * @return Durchschnittliche RTT
     */
    public synchronized long getAverageRTT() {
        long sum = 0;
        int nrClients = 0;

        for (int i = 0; i < numberOfClients; i++) {
            // Nur Threads, die mindestens eine Antwort bekommen haben, verwenden
            if (clientStatistics[i].receivedResponses > 0) {
                sum = sum + clientStatistics[i].averageRTT;
                nrClients++;
            }
        }

        if (nrClients > 0)
            return (sum / nrClients);
        else
            return 0;
    }

    public long getAverageRTT(int i) {
        return clientStatistics[i].averageRTT;
    }

    /**
     * Minimale RTT ueber alle Clients ermitteln
     *
     * @return Minimale RTT
     */
    public synchronized long getMinimumRTT() {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < numberOfClients; i++) {
            // Nur Threads, die mindestens eine Antwort bekommen haben, verwenden
            if (clientStatistics[i].receivedResponses > 0) {
                min = Math.min(clientStatistics[i].averageRTT, min);
            }
        }

        if (min == Long.MAX_VALUE){
        	return 0;
        } else {       	
        	return min;
        }
    }

    /**
     * Maximale RTT ueber alle Clients ermitteln
     *
     * @return Maximale RTT
     */
    public synchronized long getMaximumRTT() {
        long max = -1;

        for (int i = 0; i < numberOfClients; i++) {
            // Nur Threads, die mindestens eine Antwort bekommen haben, verwenden
            if (clientStatistics[i].receivedResponses > 0) {
                if (clientStatistics[i].averageRTT > max) {
                    max = clientStatistics[i].averageRTT;
                }
            }
        }

        if (max == -1){
        	return 0;
        } else {       	
        	return max;
        }
    }

    /**
     * Anzahl der gesendeten Requests aller Clients liefern
     *
     * @return Anzahl gesendeter Requests
     */
    public synchronized int getNumberOfSentRequests() {
        int sum = 0;

        for (int i = 0; i < numberOfClients; i++) {
            sum += clientStatistics[i].sentRequests;
        }

        return sum;
    }
    
    /**
     * Anzahl der gesendeten Requests eines Clients liefern
     *
     * @param i Client-Id
     * @return Anzahl gesendeter Requests des Clients i
     */
    public synchronized int getNumberOfSentRequests(int i) {
        if (!inRange(i)) return (-1);
        return clientStatistics[i].sentRequests;
    }

    /**
     * Anzahl der empfangenden Responses liefern
     *
     * @return Anzahl empfangener Responses
     */
    public synchronized int getNumberOfReceivedResponses() {
        int sum = 0;

        for (int i = 0; i < numberOfClients; i++) {
            sum += clientStatistics[i].receivedResponses;
        }

        return sum;
    }

    /**
     * Anzahl der empfangenen Responses eines Clients liefern
     *
     * @param i Client-Id
     * @return Anzahl empfangenerResponses des Clients i
     */
    public synchronized int getNumberOfReceivedResponses(int i) {
        if (!inRange(i)) return (-1);
        return clientStatistics[i].receivedResponses;
    }

    /**
     * Anzahl der verlorenen Responses liefern
     *
     * @return Anzahl verlorenen Responses
     */
    public synchronized int getNumberOfLostResponses() {
        int sum = 0;

        for (int i = 0; i < numberOfClients; i++) {
            sum += clientStatistics[i].sentRequests;
        }

        return sum - getNumberOfReceivedResponses();
    }

    /**
     * Anzahl der verlorenen Responses eines Clients liefern
     *
     * @param i Client-Id
     * @return Anzahl verlorenen Responses des Clients i
     */
    public synchronized int getNumberOfLostResponses(int i) {
        if (!inRange(i)) return (-1);
        return clientStatistics[i].sentRequests - getNumberOfReceivedResponses(i);
    }

    /**
     * Anzahl der Uebertragungswuederholungen eines Clients liefern
     *
     * @param i Client-Id
     * @return Anzahl Uebertragungswiederholungen des Clients i
     */
    public synchronized int getNumberOfRetries(int i) {
        if (!inRange(i)) return (-1);
        return clientStatistics[i].numberOfRetries;
    }

    /**
     * Gesamte RTT ueber einen Client ermitteln
     *
     * @return RTT
     */
    public synchronized long getSumRTT(int i) {
        if (!inRange(i)) return (-1);
        return clientStatistics[i].sumRTT;
    }

    /**
     * Gesamte RTT ueber alle Clients ermitteln
     *
     * @return RTT
     */
    public synchronized long getSumRTT() {
        long sum = 0;

        for (int i = 0; i < numberOfClients; i++) {
            sum += clientStatistics[i].sumRTT;
        }

        return sum;
    }

    /**
     * Serverzeit eines Clients ermitteln
     *
     * @return Serverzeit
     */
    public synchronized long getSumServerTime(int i) {
        if (!inRange(i)) return (-1);
        return clientStatistics[i].sumServerTime;
    }

    /**
     * Gesamte Serverzeit ueber alle Clients ermitteln
     *
     * @return Serverzeit
     */
    public synchronized long getSumServerTime() {
        long sum = 0;

        for (int i = 0; i < numberOfClients; i++) {
            sum += clientStatistics[i].sumServerTime;
        }

        return sum;
    }

    /**
     * Durchschnittliche Serverbearbeitungszeit ermitteln
     *
     * @return Serverbearbeitungszeit
     */
    public synchronized long getAverageServerTime() {
        long sum = 0;
        int nrClients = 0;

        for (int i = 0; i < numberOfClients; i++) {
            // Nur Threads, die mindestens eine Antwort bekommen haben, verwenden
            if (clientStatistics[i].receivedResponses > 0) {
                sum = sum + clientStatistics[i].sumServerTime;
                nrClients++;
            }
        }

        if (nrClients > 0)
            return (sum / nrClients);
        else
            return 0;
    }

    /**
     * Maximale Heap-Groesse ueber alle Clients ermitteln
     *
     * @return Maximale Heap-Groesse
     */
    public synchronized long getMaxHeapSize() {
        long max = -1;

        for (int i = 0; i < numberOfClients; i++) {
            // Nur Threads, die mindestens eine Antwort bekommen haben, verwenden
            if (clientStatistics[i].receivedResponses > 0) {
                if (clientStatistics[i].maxHeapSize > max) {
                    max = clientStatistics[i].maxHeapSize;
                }
            }
        }

        return max;
    }

    /**
     * Ausgabe Statistikdaten fuer einen Client
     *
     * @param i Client-Id
     */
    public synchronized void printClientStatistic(int i) {
        if (!inRange(i)) return;

        System.out.println("********************** Client-Statistik *****************************"
                + "\n" + "Sende-/Empfangsstatistik des Clients mit Id " + i + " (" + Thread.currentThread().getName() + ")"
                + "\n" + "Anzahl Requests gesendet: " + this.getNumberOfSentRequests(i)
                + "\n" + "Anzahl empfangener Responses: " + this.getNumberOfReceivedResponses(i)
                + "\n" + "Anzahl verlorener Responses: " + this.getNumberOfLostResponses(i)
                + "\n" + "Anzahl Uebertragungswiederholungen: " + this.getNumberOfRetries(i)                
                + "\n" + "Anzahl aller gesendeten Events: " + this.getNumberOfSentEventMessages(i)
                + "\n" + "Anzahl aller gesendeten Events-Confirms: " + this.getNumberOfReceivedConfirmEvents(i)
                + "\n" + "Anzahl aller nicht empfangenen Event-Confirms: " + this.getNumberOfLostConfirmEvents(i)                                   
                + "\n" + "Anzahl aller Event-Wiederholungen: " + this.getNumberOfRetriedEvents(i)               
                + "\n" + "Durchschnittliche RTT: " + this.getAverageRTT(i) + " ns = " + this.getAverageRTT(i) / 1000000 + " ms"
                + "\n" + "Gesamte RTT: " + this.getSumRTT(i) + " ns = " + this.getSumRTT(i) / 1000000 + " ms"
                + "\n" + "Gesamte Serverzeit: " + this.getSumServerTime(i) + " ns = " + this.getSumServerTime(i) / 1000000 + " ms"
                + "\n" + "Gesamte Kommunikationszeit: " + (this.getSumRTT(i) - this.getSumServerTime(i)) + " ns = " +
                (this.getSumRTT(i) - this.getSumServerTime(i) / 1000000) + " ms"
                + "\n" + "********************** Ende Client-Statistik ************************");
    }

    /**
     * Ausgabe aller Statistikdaten
     */
    public synchronized void printStatistic() {

        NumberFormat n = NumberFormat.getInstance();
        //n.setMaximumFractionDigits(2);
        String usedMemoryAsString = n.format(usedMemory() / (1024 * 1024));

        System.out.println("*********************************************************************"
                + "\n" + "***************************** Statistik *****************************"
                + "\n" + "Geplante Requests: " + numberOfAllMessages
                + "\n" + "Anzahl Clients: " + numberOfClients
                + "\n" + "Denkzeit des Clients zwischen zwei Requests: " + clientThinkTime + " ms"
                + "\n" + "Anzahl gesendeter Requests: " + this.getNumberOfSentRequests()
                + "\n" + "Anzahl empfangener Responses: " + this.getSumOfAllReceivedMessages() + " von erwarteten " + numberOfAllMessages
                + "\n" + "Anzahl Uebertragungswiederholungen: " + this.getSumOfAllRetries()
                + "\n" + "Anzahl geplanter Events (nur fuer Chat-Nachrichten): " + numberOfPlannedEventMessages               
                + "\n" + "Anzahl aller gesendeten Events: " + this.getSumOfAllSentEventMessages()
                + "\n" + "Anzahl aller gesendeten Events-Confirms: " + this.getSumOfAllReceivedConfirmEvents()
                + "\n" + "Anzahl aller nicht empfangenen Event-Confirms: " + this.getSumOfAllLostConfirmEvents()                                   
                + "\n" + "Anzahl aller Event-Wiederholungen: " + this.getSumOfAllRetriedEvents()                                               
                + "\n"
                + "\n" + "Gesamte RTT ueber alle Clients: " + this.getSumRTT() + " ns ("
                + (this.getSumRTT() / 1000000) + " ms)"
                + "\n" + "Gesamte Serverzeit ueber alle Clients: " + this.getSumServerTime() + " ns  (" +
                (this.getSumServerTime() / 1000000) + " ms)"
                + "\n" + "Reine Kommunikationszeit ueber alle Clients: " + (this.getSumRTT() - this.getSumServerTime()) + " ns (" +
                ((this.getSumRTT() - this.getSumServerTime()) / 1000000) + " ms)"
                + "\n\n" + "Durchschnittswerte ueber alle Clients:"
                + "\n" + "RTT: " + this.getAverageRTT() + " ns (" + this.getAverageRTT() / 1000000 + " ms)"
                + "\n" + "Minimum RTT: " + this.getMinimumRTT() + " ns (" + this.getMinimumRTT() / 1000000 + " ms)"
                + "\n" + "Maximum RTT: " + this.getMaximumRTT() + " ns (" + this.getMaximumRTT() / 1000000 + " ms)"
                + "\n" + "Reine Serverzeit: " + this.getAverageServerTime() / numberOfClients + " ns (" +
                (this.getAverageServerTime() / numberOfClients) / 1000000 + " ms)"
                + "\n" + "Maximal erreichte Heap-Belegung: " + usedMemoryAsString + " MByte"

                + "\n" + "************************ Ende Statistik *****************************"
                + "\n" + "*********************************************************************");
    }

    /**
     * Ausgabe eines Auswertungssatzes fuer eine Messung (einen Benchmark-Lauf)
     * in eine Datei im CSV-Dateiformat in folgender Form:
     * <p/>
     * 
     * 01 Messungstyp als String
     * <p/>
     *  02 Implementierungstyp als String
     * <p/>
     * 03 Anzahl Client-Threads
     * <p/>
     * 04 Anzahl (geplante) Nachrichten je Client
     * <p/>
     * 05 Durchschnittliche RTT
     * <p/>
     * 06 Maximale RTT
     * <p/>
     * 07 Minimale RTT
     * <p/>
     * 08 Durchschnittliche Serverbearbeitungszeit
     * <p/>
     * 09 Anzahl geplanter Requests/Responses
     * <p/>
     * 10 Anzahl gesendeter Requests
     * <p/>
     * 11 Anzahl empfangener Responses
     * <p/>
     * 12 Anzahl verlorener Echo Responses
     * <p/>
     * 13 Anzahl aller Uebertragungswiederholungen (nur fuer UDP relevant)
     * <p/>
     * 14 Anzahl aller vom Server gesendete Events-Nachrichten
     * <p/>
	 * 15 Anzahl aller vom Server empfangenen Event-Bestaetigungen (Confirm-Event)
	 * <p/>
	 * 16 Anzahl aller vom Server nicht erhaltenen Event-Bestaetigungen
	 * <p/>
	 * 17 Anzahl aller Wiederholungen von Events (fuer unzuverlaessige Verbindungen wie UDP)
	 * <p/>
     * 18 Maximale Heap-Size des Clients
     * <p/>
     * 19 Startzeit der Messung
     * <p/>
     * 20 Endezeit der Messung
     * <p/>
     * Der Satz wird an das Ende einer bestehenden Datei angehaengt.
     * <p/>
     * Die Datei kann zur Testauswertung in Excel weiterverarbeitet werden.
     *
     * @param fileName    Name der Datei
     * @param implType    Typ der Implementierung
     * @param measureType Typ der Messung
     * @param startTime   der Messung
     * @param endTime     der Messung
     */

    public synchronized void writeStatisticSet
    (String fileName, String implType, String measureType, String startTime, String endTime) {
        File file = new File(fileName);
        final String newLine = "\r\n";

        try {
            boolean exist = file.createNewFile();
            if (!exist) {
                log.debug("Datei " + fileName + " existierte bereits");
            } else {
                log.debug("Datei " + fileName + " erfolgreich angelegt");
            }

            // Datei zum Erweitern oeffnen
            FileWriter fstream = new FileWriter(fileName, true);
            BufferedWriter out = new BufferedWriter(fstream);

            out.write(measureType + ", " +
                    implType + ", " +
                    numberOfClients + ", " +
                    numberOfMessages + ", " +
                    this.getAverageRTT() / 1000000 + ", " +
                    this.getMaximumRTT() / 1000000 + ", " +
                    this.getMinimumRTT() / 1000000 + ", " +
                    this.getAverageServerTime() / 1000000 + ", " +
                    numberOfAllMessages + ", " +
                    this.getNumberOfSentRequests() + ", " +
                    this.getNumberOfReceivedResponses() + ", " +              
                    this.getNumberOfLostResponses() + ", " +                  
                    this.getSumOfAllRetries() + ", " +                    
 					this.getSumOfAllSentEventMessages() + ", " +
 					this.getSumOfAllReceivedConfirmEvents() + ", " +
 					this.getSumOfAllLostConfirmEvents() + ", " +
 					this.getSumOfAllRetriedEvents() + ", " +                   
                    this.getMaxHeapSize() / (1024 * 1024) + ", " +
                    startTime + ", " +
                    endTime +
                    newLine);
            
            System.out.println("Auswertungssatz in Datei " + fileName + " geschrieben");

            out.flush();
            out.close();

        } catch (IOException e) {
            log.error("Fehler beim Schreiben des Auswertungssatzes in Datei " + fileName);
        }
    }

    /**
     * Berechnet den tatsaechlich benutzten Heap-Speicher
     * Heap-Groesse in MByte
     */
    private long usedMemory() {
        Runtime r = Runtime.getRuntime();
        long usedMemory = (r.totalMemory() - r.freeMemory());
        return (usedMemory);
    }
}