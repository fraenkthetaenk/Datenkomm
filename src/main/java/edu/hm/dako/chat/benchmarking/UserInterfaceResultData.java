package edu.hm.dako.chat.benchmarking;

import javax.swing.JFormattedTextField;

public class UserInterfaceResultData {

    private long numberOfSentRequests;  // Anzahl gesendeter Requests
    private long numberOfResponses;     // Anzahl empfangener Responses
    private long numberOfLostResponses; // Anzahl verlorener (nicht empfangener) Responses
    private long numberOfRetries;		// Anzahl an Wiederholungen beim Senden
    private String endTime;             // Testende als Datum/Uhrzeit-String
    private long elapsedTime;           // Testdauer in Sekunden
    
    private long numberOfSentEventMessages; // Anzahl gesendeter Event-Nachrichten im Server
    private long numberOfReceivedConfirmEvents; // Anzahl empfangener Event-Bestaetigungen im Server
    private long numberOfLostConfirmEvents; // Anzahl verlorener Bestaetigungen im Server
    private long numberOfRetriedEvents; // Anzahl von erneuten Sendungen von Event-Nachrichten im Server
    
    private double avgRTT;              // Mittlere RTT in ms
    private double maxRTT;              // Maximale RTT in ms
    private double minRTT;              // Minimale RTT in ms
    private double avgServerTime;       // Mittlere Serverbearbeitungszeit in ms
    private long maxHeapSize;           // Maximale Heap-Belegung waehrend des Testlaufs in MByte
    private float maxCpuUsage;          // Maximale CPU-Auslastung waehrend des Testlaufs in Prozent


    public long getNumberOfSentRequests() {
        return numberOfSentRequests;
    }

    public void setNumberOfSentRequests(long numberOfSentRequests) {
        this.numberOfSentRequests = numberOfSentRequests;
    }

    public long getNumberOfResponses() {
        return numberOfResponses;
    }

    public void setNumberOfResponses(long numberOfResponses) {
        this.numberOfResponses = numberOfResponses;
    }

    public long getNumberOfLostResponses() {
        return numberOfLostResponses;
    }
    
    public void setNumberOfLostResponses(long numberOfLostResponses) {
        this.numberOfLostResponses = numberOfLostResponses;
    }

    public void setNumberOfRetries(long numberOfRetries) {
        this.numberOfRetries = numberOfRetries;
    }
    
    public long getNumberOfRetries() {
        return numberOfRetries;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public void setNumberOfSentEventMessages(long numberOfSentEventMessages) {
        this.numberOfSentEventMessages = numberOfSentEventMessages;
    }
    
    public long getNumberOfSentEventMessages() {
        return numberOfSentEventMessages;
    }

    public void setNumberOfReceivedConfirmEvents(long numberOfReceivedConfirmEvents) {
        this.numberOfReceivedConfirmEvents = numberOfReceivedConfirmEvents;
    }
    
    public long getNumberOfReceivedConfirmEvents() {
        return numberOfReceivedConfirmEvents;
    }

    public void setNumberOfLostConfirmEvents(long numberOfLostConfirmEvents) {
        this.numberOfLostConfirmEvents = numberOfLostConfirmEvents;
    }
    
    public long getNumberOfLostConfirmEvents() {
        return numberOfLostConfirmEvents;
    }

    public void setNumberOfRetriedEvents(long numberOfRetriedEvents) {
        this.numberOfRetriedEvents = numberOfRetriedEvents;
    }
    
    public long getNumberOfRetriedEvents() {
        return numberOfRetriedEvents;
    }
    
    public double getAvgRTT() {
        return avgRTT;
    }

    public void setAvgRTT(double avgRTT) {
        this.avgRTT = avgRTT;
    }

    public double getMaxRTT() {
        return maxRTT;
    }

    public void setMaxRTT(double maxRTT) {
        this.maxRTT = maxRTT;
    }

    public double getMinRTT() {
        return minRTT;
    }

    public void setMinRTT(double minRTT) {
        this.minRTT = minRTT;
    }

    public double getAvgServerTime() {
        return avgServerTime;
    }

    public void setAvgServerTime(double avgServerTime) {
        this.avgServerTime = avgServerTime;
    }

    public long getMaxHeapSize() {
        return maxHeapSize;
    }

    public void setMaxHeapSize(long maxHeapSize) {
        this.maxHeapSize = maxHeapSize;
    }

    public double getMaxCpuUsage() {
        return maxCpuUsage;
    }

    public void setMaxCpuUsage(float maxCpuUsage) {
        this.maxCpuUsage = maxCpuUsage;
    }
}