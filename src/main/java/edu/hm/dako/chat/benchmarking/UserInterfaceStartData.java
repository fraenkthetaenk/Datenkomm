package edu.hm.dako.chat.benchmarking;

public class UserInterfaceStartData {

    long numberOfRequests;      		// Anzahl geplanter Requests
    String startTime;           		// Zeit des Testbeginns   
    long numberOfPlannedEventMessages;  // Anzahl der geplanten Event-Nachrichten


    public long getNumberOfRequests() {
        return numberOfRequests;
    }

    public void setNumberOfRequests(long numberOfRequests) {
        this.numberOfRequests = numberOfRequests;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }
    
    public long getNumberOfPlannedEventMessages() {
        return numberOfPlannedEventMessages;
    }

    public void setNumberOfPlannedEventMessages(long numberOfPlannedEventMessages) {
        this.numberOfPlannedEventMessages = numberOfPlannedEventMessages;
    }

}