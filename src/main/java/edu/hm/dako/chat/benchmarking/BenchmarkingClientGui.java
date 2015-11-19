/**
 * 
 * Sehr einfaches und noch nicht ausgereiftes Swing-basiertes GUI 
 * fuer den Anstoss von Testlaeufen im Benchmarking-Client
 *
 *  Ein Refactoring und eine Fehlerbereinigung (nicht Bestandteil der Studienarbeit) ist 
 *  noch durchzufuehren:
 *  - Buttons an der Oberflaeche kleiner machen
 *  - Pruefen, ob alle Eingabeparameter ordnungsgemaess erfasst wurden (Plausibilitaetspruefung)
 * 	- Namensgebung fuer Variablen verbessern
 * 	- Compiler-Warnungs entfernen
 * 	- ImplementationType und MeasurementType besser erfassen (redundanten Code vermeiden)
 * 	- Problem bei langen Tests - GUI hat nach Fensterwechsel nicht mehr den Fokus - beseitigen
 * 	- Fehlerbereinigung insgesamt
 */
package edu.hm.dako.chat.benchmarking;

//TODO Felder des Bildschirms bei erneutem Darstellen in den Vordergrund bringen

import com.jgoodies.forms.factories.Borders;
import com.jgoodies.forms.factories.DefaultComponentFactory;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;

import edu.hm.dako.chat.benchmarking.UserInterfaceInputParameters.ImplementationType;
import edu.hm.dako.chat.benchmarking.UserInterfaceInputParameters.MeasurementType;

import org.apache.log4j.PropertyConfigurator;

import javax.swing.*;
import javax.swing.plaf.nimbus.NimbusLookAndFeel;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.util.Formatter;

public class BenchmarkingClientGui extends JPanel
        implements BenchmarkingClientUserInterface, ActionListener {

    public static final String IMPL_TCP = "TCP";
    public static final String IMPL_UDP = "UDP";
    private long timeCounter = 0; // Zeitzaehler fuer Testlaufzeit

    private static JFrame frameBenchmarkingGui; // Frame fuer Anwendungs-GUI
    private static JPanel panelBenchmarkingClientGui;

    /**
     * GUI-Komponenten
     */

    private JComboBox optionListImplType;
    private JComboBox optionListMeasureType;
    private JTextField textFieldNumberOfClientThreads;
    private JFormattedTextField textFieldAvgCpuUsage;
    private JFormattedTextField textFieldNumberOfMessagesPerClients;
    private JTextField textFieldServerport;
    private JTextField textFieldThinkTime;
    private JFormattedTextField textFieldServerIpAddress;
    private JFormattedTextField textFieldMessageLength; 
    private JFormattedTextField textFieldNumberOfMaxRetries;
    private JFormattedTextField textFieldResponseTimeout;
    private JFormattedTextField textFieldSeperator;
    private JFormattedTextField textFieldPlannedRequests;
    private JFormattedTextField textFieldTestBegin;
    private JFormattedTextField textFieldSentRequests;
    private JFormattedTextField textFieldTestEnd;
    private JFormattedTextField textFieldReceivedResponses;
    private JFormattedTextField textFieldTestDuration;
    private JFormattedTextField textFieldAvgRTT;
    private JFormattedTextField textFieldAvgServerTime;
    private JFormattedTextField textFieldMaxRTT;
    private JFormattedTextField textFieldMaxHeapUsage;
    private JFormattedTextField textFieldMinRTT;
    private JFormattedTextField textFieldNumberOfRetries; // Anzahl der Uebertragungswiederholungen
    
    private JFormattedTextField textFieldPlannedEventMessages;
    private JFormattedTextField textFieldSentEventMessages;
    private JFormattedTextField textFieldReceivedConfirmEvents;
    private JFormattedTextField textFieldLostConfirmEvents;
    private JFormattedTextField textFieldRetriedEvents;
    

    private JTextArea messageArea;
    private JScrollPane scrollPane;

    private Button startButton;
    private Button newButton;
    private Button finishButton;

    private static final long serialVersionUID = 100001000L;
	private Formatter formatter;

    public BenchmarkingClientGui() {
          super(new BorderLayout());
    }

    private void initComponents() {

        /**
         * Erzeugen der GUI-Komponenten
         */
        String[] optionStrings = {
                IMPL_TCP,
                IMPL_UDP};
        optionListImplType = new JComboBox(optionStrings);

        String[] optionStrings1 = {
                "Variable Threads",
                "Variable Length"};
        optionListMeasureType = new JComboBox(optionStrings1);

        textFieldNumberOfClientThreads = new JTextField();
        //text = new JFormattedTextField();
        textFieldAvgCpuUsage = new JFormattedTextField();
        //textFieldNumberOfMessages = new JFormattedTextField();
        textFieldNumberOfMessagesPerClients = new JFormattedTextField();
        textFieldServerport = new JTextField();
        textFieldThinkTime = new JTextField();
        textFieldServerIpAddress = new JFormattedTextField();
        textFieldMessageLength = new JFormattedTextField();
        
        textFieldNumberOfMaxRetries = new JFormattedTextField();
        textFieldResponseTimeout = new JFormattedTextField();
        
        textFieldSeperator = new JFormattedTextField();
        textFieldPlannedRequests = new JFormattedTextField();
        textFieldTestBegin = new JFormattedTextField();
        textFieldSentRequests = new JFormattedTextField();
        textFieldTestEnd = new JFormattedTextField();
        textFieldReceivedResponses = new JFormattedTextField();
        textFieldTestDuration = new JFormattedTextField();
        textFieldAvgRTT = new JFormattedTextField();
        textFieldAvgServerTime = new JFormattedTextField();
        textFieldMaxRTT = new JFormattedTextField();
        textFieldMaxHeapUsage = new JFormattedTextField();
        textFieldMinRTT = new JFormattedTextField();   
        

        textFieldNumberOfRetries = new JFormattedTextField(); // Anzahl der Uebertragungswiederholungen
        
        textFieldPlannedEventMessages = new JFormattedTextField();       
        textFieldPlannedEventMessages = new JFormattedTextField();
        textFieldSentEventMessages = new JFormattedTextField();
        textFieldReceivedConfirmEvents = new JFormattedTextField();
        textFieldLostConfirmEvents = new JFormattedTextField();
        textFieldRetriedEvents = new JFormattedTextField();
        
        

        // Nachrichtenbereich mit Scrollbar
        messageArea = new JTextArea("", 5, 100);

        //messageArea.setLineWrap(true);
        scrollPane = new JScrollPane(messageArea);
        scrollPane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);

        // Buttons
        startButton = new Button("Starten");
        newButton = new Button("Neu");
        finishButton = new Button("Beenden");
    }

    /**
     * buildPanel
     * Panel anlegen
     *
     * @return
     */
    public JComponent buildPanel() {

        initComponents();

        FormLayout layout = new FormLayout( 
        		// Spalten
                "right:max(40dlu;pref), 3dlu, 70dlu, 7dlu, right:max(40dlu;pref), 3dlu, 70dlu", 
                // Zeilen
                "p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p," + // Erster Block, Eingabepearameter
                "20dlu, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p," + // Zweiter Block, Laufzeitdaten
                "10dlu, p, p, p, p, p, p, p, p, p," + // Dritter Block, Messergebnisse
                "20dlu, p, p, p," + // Vierter Block, Meldungszeilen
                "p, p, p, p" // Fuenfter Block, Buttons
        		);

        panelBenchmarkingClientGui = new JPanel(layout);
        panelBenchmarkingClientGui.setBorder(Borders.DIALOG_BORDER);

        /*
         *  Panel mit Labels und Komponenten fuellen
         */

        CellConstraints cc = new CellConstraints();
        panelBenchmarkingClientGui.add(createSeparator("Eingabeparameter"), cc.xyw(1, 1, 7));
        panelBenchmarkingClientGui.add(new JLabel("Implementierungsstyp"), cc.xy(1, 3));
        panelBenchmarkingClientGui.add(optionListImplType, cc.xyw(3, 3, 1));
        
        panelBenchmarkingClientGui.add(new JLabel("Anzahl Client-Threads"), cc.xy(5, 3));
        panelBenchmarkingClientGui.add(textFieldNumberOfClientThreads, cc.xy(7, 3));
        textFieldNumberOfClientThreads.setText("1");
        
        panelBenchmarkingClientGui.add(new JLabel("Art der Messung"), cc.xy(1, 5));
        panelBenchmarkingClientGui.add(optionListMeasureType, cc.xyw(3, 5, 1));
        
        panelBenchmarkingClientGui.add(new JLabel("Anzahl Nachrichten je Client"), cc.xy(5, 5));
        panelBenchmarkingClientGui.add(textFieldNumberOfMessagesPerClients, cc.xy(7, 5));
        textFieldNumberOfMessagesPerClients.setText("10");
        
        panelBenchmarkingClientGui.add(new JLabel("Serverport"), cc.xy(1, 7));
        panelBenchmarkingClientGui.add(textFieldServerport, cc.xy(3, 7));
        textFieldServerport.setText("50000");
        
        panelBenchmarkingClientGui.add(new JLabel("Denkzeit [ms]"), cc.xy(5, 7));
        panelBenchmarkingClientGui.add(textFieldThinkTime, cc.xy(7, 7));
        textFieldThinkTime.setText("100");
        
        panelBenchmarkingClientGui.add(new JLabel("Server-IP-Adresse"), cc.xy(1, 9));
        panelBenchmarkingClientGui.add(textFieldServerIpAddress, cc.xy(3, 9));
        textFieldServerIpAddress.setText("localhost");
        
        panelBenchmarkingClientGui.add(new JLabel("Nachrichtenlaenge [Byte]"), cc.xy(5, 9));
        panelBenchmarkingClientGui.add(textFieldMessageLength, cc.xy(7, 9));
        textFieldMessageLength.setText("10");
  
        panelBenchmarkingClientGui.add(new JLabel("Max. Anzahl Wiederholungen"), cc.xy(1, 11));
        panelBenchmarkingClientGui.add(textFieldNumberOfMaxRetries, cc.xy(3, 11));
        textFieldNumberOfMaxRetries.setText("1");
        
        panelBenchmarkingClientGui.add(new JLabel("Response-Timeout [ms]"), cc.xy(5, 11));
        panelBenchmarkingClientGui.add(textFieldResponseTimeout, cc.xy(7, 11));
        textFieldResponseTimeout.setText("2000");

        panelBenchmarkingClientGui.add(createSeparator("Laufzeitdaten"), cc.xyw(1, 17, 7));
        panelBenchmarkingClientGui.add(new JLabel("Geplante Requests"), cc.xy(1, 19));
        panelBenchmarkingClientGui.add(textFieldPlannedRequests, cc.xy(3, 19));
        textFieldPlannedRequests.setEditable(false);
        
        panelBenchmarkingClientGui.add(new JLabel("Testbeginn"), cc.xy(5, 19));
        panelBenchmarkingClientGui.add(textFieldTestBegin, cc.xy(7, 19));
        textFieldTestBegin.setEditable(false);
        
        panelBenchmarkingClientGui.add(new JLabel("Gesendete Requests"), cc.xy(1, 21));
        panelBenchmarkingClientGui.add(textFieldSentRequests, cc.xy(3, 21));
        textFieldSentRequests.setEditable(false);
        
        panelBenchmarkingClientGui.add(new JLabel("Testende"), cc.xy(5, 21));
        panelBenchmarkingClientGui.add(textFieldTestEnd, cc.xy(7, 21));
        textFieldTestEnd.setEditable(false);
        
        panelBenchmarkingClientGui.add(new JLabel("Empfangene Responses"), cc.xy(1, 23));
        panelBenchmarkingClientGui.add(textFieldReceivedResponses, cc.xy(3, 23));
        textFieldReceivedResponses.setEditable(false);
        
        panelBenchmarkingClientGui.add(new JLabel("Testdauer [s]"), cc.xy(5, 23));
        panelBenchmarkingClientGui.add(textFieldTestDuration, cc.xy(7, 23));
        textFieldTestDuration.setEditable(false);
        
        panelBenchmarkingClientGui.add(new JLabel("Max. geplante Event-Nachrichten"), cc.xy(1, 26));
        panelBenchmarkingClientGui.add(textFieldPlannedEventMessages, cc.xy(3, 26));
        textFieldPlannedEventMessages.setEditable(false);
        
        panelBenchmarkingClientGui.add(new JLabel("Gesendete Event-Nachrichten"), cc.xy(5, 26));
        panelBenchmarkingClientGui.add(textFieldSentEventMessages, cc.xy(7, 26));
        textFieldSentEventMessages.setEditable(false);
        
        panelBenchmarkingClientGui.add(new JLabel("Empfangene Confirm-Nachrichten"), cc.xy(1, 28));
        panelBenchmarkingClientGui.add(textFieldReceivedConfirmEvents, cc.xy(3, 28));
        textFieldReceivedConfirmEvents.setEditable(false);
        
        panelBenchmarkingClientGui.add(new JLabel("Verlorene Confirm-Nachrichten"), cc.xy(5, 28));
        panelBenchmarkingClientGui.add(textFieldLostConfirmEvents, cc.xy(7, 28));
        textFieldLostConfirmEvents.setEditable(false);
        
        panelBenchmarkingClientGui.add(new JLabel("Wiederholte Event-Nachrichten"), cc.xy(1, 30));
        panelBenchmarkingClientGui.add(textFieldRetriedEvents, cc.xy(3, 30));
        textFieldRetriedEvents.setEditable(false);
        
        panelBenchmarkingClientGui.add(new JLabel("Anzahl aller Uebertragungswiederholungen"), cc.xy(5, 30));
        panelBenchmarkingClientGui.add(textFieldNumberOfRetries, cc.xy(7, 30));
        textFieldNumberOfRetries.setEditable(false);        
        
        panelBenchmarkingClientGui.add(createSeparator("Messergebnisse"), cc.xyw(1, 37, 7));
        
        panelBenchmarkingClientGui.add(new JLabel("Mittlere RTT [ms]"), cc.xy(1, 39));
        panelBenchmarkingClientGui.add(textFieldAvgRTT, cc.xy(3, 39));
        textFieldAvgRTT.setEditable(false);
        
        panelBenchmarkingClientGui.add(new JLabel("Mittlere Serverzeit [ms]"), cc.xy(5, 39));
        panelBenchmarkingClientGui.add(textFieldAvgServerTime, cc.xy(7, 39));
        textFieldAvgServerTime.setEditable(false);
        
        panelBenchmarkingClientGui.add(new JLabel("Maximale RTT [ms]"), cc.xy(1, 41));
        panelBenchmarkingClientGui.add(textFieldMaxRTT, cc.xy(3, 41));
        textFieldMaxRTT.setEditable(false);
        
        panelBenchmarkingClientGui.add(new JLabel("Maximale Heap-Belegung [MiB]"), cc.xy(5, 41));
        panelBenchmarkingClientGui.add(textFieldMaxHeapUsage, cc.xy(7, 41));
        textFieldMaxHeapUsage.setEditable(false);
        
        panelBenchmarkingClientGui.add(new JLabel("Minimale RTT [ms]"), cc.xy(1, 43));
        panelBenchmarkingClientGui.add(textFieldMinRTT, cc.xy(3, 43));
        textFieldMinRTT.setEditable(false);
        
        panelBenchmarkingClientGui.add(new JLabel("Durchschnittliche CPU-Auslastung [%]"), cc.xy(5, 43));
        panelBenchmarkingClientGui.add(textFieldAvgCpuUsage, cc.xy(7, 43));
        textFieldAvgCpuUsage.setEditable(false);
      
        
        panelBenchmarkingClientGui.add(createSeparator(""), cc.xyw(1, 47, 7));

        // Meldungsbereich erzeugen

        panelBenchmarkingClientGui.add(scrollPane, cc.xyw(1, 49, 7));
        messageArea.setLineWrap(true);
        messageArea.setWrapStyleWord(true);
        messageArea.setEditable(false);
        messageArea.setCaretPosition(0);

        panelBenchmarkingClientGui.add(createSeparator(""), cc.xyw(1, 51, 7));

        // Buttons erzeugen
        panelBenchmarkingClientGui.add(startButton, cc.xyw(2, 53, 2));   //Starten
        panelBenchmarkingClientGui.add(newButton, cc.xyw(4, 53, 2));     //Loeschen
        panelBenchmarkingClientGui.add(finishButton, cc.xyw(6, 53, 2));  //Abbrechen
        
        // Listener fuer Buttons registrieren
        startButton.addActionListener(this);
        newButton.addActionListener(this);
        finishButton.addActionListener(this);
        return panelBenchmarkingClientGui;
    }

    /**
     * actionPerformed
     * Listener-Methode zur Bearbeitung der Button-Aktionen
     * Reagiert auf das Betaetigen eines Buttons
     *
     * @param e Ereignis
     */
    //@SuppressWarnings("deprecation")
    public void actionPerformed(ActionEvent e) {

        // Analysiere Ereignis und fuehre entsprechende Aktion aus

        if (e.getActionCommand().equals("Starten")) {
            startAction(e);
            startButton.setEnabled(false);
        } else if (e.getActionCommand().equals("Neu")) {
            newAction(e);
            startButton.setEnabled(true);
        } else if (e.getActionCommand().equals("Beenden"))
            finishAction(e);
    }

    /**
     * startAction
     * Aktion bei Betaetigung des "Start"-Buttons ausfuehren
     * Eingabefelder werden validiert.
     *
     * @param e Ereignis
     */
    private void startAction(ActionEvent e) {
        // Input-Parameter aus GUI lesen
        UserInterfaceInputParameters iParm = new UserInterfaceInputParameters();

        String testString;
        
        /*
         *  GUI sammmelt Eingabedaten und validieren
         */

        // Validierung fuer Denkzeit
        testString = textFieldThinkTime.getText();
        if (!testString.matches( "[0-9]+")) {
        	 // nicht numerisch 
        	 // Aktualisieren des Frames auf dem Bildschirm
        	 setMessageLine("Denkzeit bitte numerisch angeben");
            frameBenchmarkingGui.update(frameBenchmarkingGui.getGraphics());
            return;
         } else {
           	Integer iThinkTime = new Integer(textFieldThinkTime.getText());
        	System.out.println("Denkzeit: " + iThinkTime + " ms");
        	iParm.setClientThinkTime(iThinkTime.intValue());
    	}

        // Validierung fuer Serverport
        testString = textFieldServerport.getText();
        if (testString.matches( "[0-9]+")){
        	Integer iServerPort = new Integer(textFieldServerport.getText());
        	if ((iServerPort < 1) || (iServerPort > 65535 )){
        		// nicht im Wertebereich
        		// Aktualisieren des Frames auf dem Bildschirm
        		setMessageLine("Serverport im Bereich von 1 bis 65535 angeben");
        		frameBenchmarkingGui.update(frameBenchmarkingGui.getGraphics());
        		return;
        	} else {
        		System.out.println("Serverport: " + iServerPort);
        		iParm.setRemoteServerPort(iServerPort.intValue());
        	}
         } else {
        	 setMessageLine("Serverport nicht numerisch");
     		 frameBenchmarkingGui.update(frameBenchmarkingGui.getGraphics());
     		 return;
         }
        
        // Validierung fuer Anzahl Client-Threads
        testString = textFieldNumberOfClientThreads.getText();
        if (testString.matches( "[0-9]+")){
        	Integer iClientThreads = new Integer(textFieldNumberOfClientThreads.getText());
        	if (iClientThreads < 1) {
        		// nicht im Wertebereich
        		// Aktualisieren des Frames auf dem Bildschirm
        		setMessageLine("Anzahl Client-Threads bitte groesser als 0 angeben");
        		frameBenchmarkingGui.update(frameBenchmarkingGui.getGraphics());
        		return;
        	} else {         	
        	 System.out.println("Anzahl Client-Threads:" + iClientThreads);
        	 iParm.setNumberOfClients(iClientThreads.intValue());
        	}
        } else {
       	 setMessageLine("Anzahl Client-Threads nicht numerisch");
    		 frameBenchmarkingGui.update(frameBenchmarkingGui.getGraphics());
    		 return;
        }
        
        // Validierung fuer Anzahl Nachrichten
        testString = textFieldNumberOfMessagesPerClients.getText();
        if (testString.matches( "[0-9]+")){
            Integer iNumberOfMessages = new Integer(textFieldNumberOfMessagesPerClients.getText());
        	if (iNumberOfMessages < 1) {
        		// nicht numerisch
        		// Aktualisieren des Frames auf dem Bildschirm
        		setMessageLine("Anzahl Nachrichten groesser als 0 angeben");
        		frameBenchmarkingGui.update(frameBenchmarkingGui.getGraphics());
        		return;
        	} else {         	
                System.out.println("Anzahl Nachrichten:" + iNumberOfMessages);
                iParm.setNumberOfMessages(iNumberOfMessages.intValue());
        	}
        } else {
       	 setMessageLine("Anzahl Nachrichten nicht numerisch");
    		 frameBenchmarkingGui.update(frameBenchmarkingGui.getGraphics());
    		 return;
        }
        	
        // Validierung fuer Nachrichtenlaenge
        testString = textFieldMessageLength.getText();
        if (testString.matches( "[0-9]+")){
        	Integer iMessageLength = new Integer(textFieldMessageLength.getText());
        	if ((iMessageLength < 1) || (iMessageLength > 50000 )){
        		// nicht im Wertebereich
        		// Aktualisieren des Frames auf dem Bildschirm
        		setMessageLine("Nachrichtenlaenge bitte im Bereich von 1 bis 50000 angeben");
        		frameBenchmarkingGui.update(frameBenchmarkingGui.getGraphics());
        		return;
        	} else {
        		 System.out.println("Nachrichtenlaenge:" + iMessageLength + " Byte");
        	     iParm.setMessageLength(iMessageLength.intValue()); 
        	}
         } else {
        	 setMessageLine("Nachrichtenlaenge nicht numerisch");
     		 frameBenchmarkingGui.update(frameBenchmarkingGui.getGraphics());
     		 return;
         }
    
        System.out.println("RemoteServerAdress:" + textFieldServerIpAddress.getText());
        iParm.setRemoteServerAddress(textFieldServerIpAddress.getText());
        
             
        // Validierung fuer Response-Timeout
        testString = textFieldResponseTimeout.getText();
        if (!testString.matches( "[0-9]+")) {
        	 // nicht numerisch 
        	 // Aktualisieren des Frames auf dem Bildschirm
        	 setMessageLine("Response-Timeout nicht numerisch");
            frameBenchmarkingGui.update(frameBenchmarkingGui.getGraphics());
            return;
         } else {
        	 Integer iResponseTimeout = new Integer(textFieldResponseTimeout.getText());
             System.out.println("Response-Timeout:" + iResponseTimeout);
             iParm.setResponseTimeout( iResponseTimeout.intValue());
    	}
        
        // Validierung fuer maximalen Nachrichtenwiederholung
        testString = textFieldNumberOfMaxRetries.getText();
        if (!testString.matches( "[0-9]+")) {
        	 // nicht numerisch 
        	 // Aktualisieren des Frames auf dem Bildschirm
        	 setMessageLine("Maximale Anzahl Wiederholungen nicht numerisch");
            frameBenchmarkingGui.update(frameBenchmarkingGui.getGraphics());
            return;
         } else {
        	 Integer iNumberOfMaxRetries = new Integer(textFieldNumberOfMaxRetries.getText());
             System.out.println("Maximale Anzahl Wiederholungen:" + iNumberOfMaxRetries);
             iParm.setNumberOfRetries( iNumberOfMaxRetries.intValue());
    	}
        
       
        /*
         *  Benchmarking-Client instanziieren und Benchmark starten
         */

        // Eingegebenen Implementierungstyp auslesen
        String item1 = (String) optionListImplType.getSelectedItem();
        System.out.println("Implementierungstyp eingegeben: " + item1);
        if (item1.equals(IMPL_TCP))
            iParm.setImplementationType(ImplementationType.TCPImplementation);
        if (item1.equals(IMPL_UDP))
            iParm.setImplementationType(ImplementationType.UDPImplementation);

        // Eingegebenen Messungstyp auslesen
        String item2 = (String) optionListMeasureType.getSelectedItem();
        System.out.println("Messungstyp eingegeben: " + item2);

        if (item1.equals("Variable Threads"))
            iParm.setMeasurementType(MeasurementType.VarThreads);
        if (item1.equals("Variable Length"))
            iParm.setMeasurementType(MeasurementType.VarMsgLength);

        // Aufruf des Benchmarks
        BenchmarkingClient benchClient = new BenchmarkingClient();
        benchClient.executeTest(iParm, this);
    }

    /**
     * newAction
     * Aktion bei Betaetigung des "Neu"-Buttons ausfuehren
     *
     * @param e Ereignis
     */
    private void newAction(ActionEvent e) {
        /*
         * Loeschen bzw. initialisieren der Ausgabefelder 
         */
        textFieldSeperator.setText("");
        textFieldPlannedRequests.setText("");
        textFieldTestBegin.setText("");
        textFieldSentRequests.setText("");
        textFieldTestEnd.setText("");
        textFieldReceivedResponses.setText("");
        textFieldTestDuration.setText("");
        textFieldAvgRTT.setText("");
        textFieldAvgServerTime.setText("");
        textFieldMaxRTT.setText("");
        textFieldAvgCpuUsage.setText("");
        textFieldMaxHeapUsage.setText("");
        textFieldMinRTT.setText("");
        textFieldNumberOfRetries.setText(""); // Anzahl an Nachrichtenwiederholungen insgesamt
        textFieldPlannedEventMessages.setText("");
        textFieldSentEventMessages.setText("");
        textFieldReceivedConfirmEvents.setText("");
        textFieldLostConfirmEvents.setText("");
        textFieldRetriedEvents.setText("");
    }

    /**
     * Aktion bei Betaetigung des "Beenden"-Buttons ausfuehren
     *
     * @param e Ereignis
     */
    private void finishAction(ActionEvent e) {
        setMessageLine("Programm wird beendet...");

        // Programm beenden
        System.exit(0);
    }

    /**
     * Schliessen des Fensters und Beenden des Programms
     *
     * @param e
     */
    public void windowClosing(WindowEvent e) {
        System.exit(0);
    }

    public void windowOpened(WindowEvent e) {
    }

    public void windowActivated(WindowEvent e) {
    }

    public void windowIconified(WindowEvent e) {
    }

    public void windowDeiconified(WindowEvent e) {
    }

    public void windowDeactivated(WindowEvent e) {
    }

    public void windowClosed(WindowEvent e) {
    }

    @Override
    public synchronized void showStartData(UserInterfaceStartData data) {
        String strNumberOfRequests = (new Long(data.getNumberOfRequests())).toString();        
        textFieldPlannedRequests.setText(strNumberOfRequests);
        
        String strNumberOfPlannedEventMessages = (new Long(data.getNumberOfPlannedEventMessages())).toString();
        textFieldPlannedEventMessages.setText(strNumberOfPlannedEventMessages);
        
        textFieldTestBegin.setText(data.getStartTime());

        // Aktualisieren der Ausgabefelder auf dem Bildschirm
        textFieldPlannedRequests.update(textFieldPlannedRequests.getGraphics());
        textFieldTestBegin.update(textFieldTestBegin.getGraphics());
        textFieldPlannedEventMessages.update(textFieldPlannedEventMessages.getGraphics());
    }

    @Override
    public synchronized void showResultData(UserInterfaceResultData data) {

    	
    	textFieldSentRequests.setText((new Long(data.getNumberOfSentRequests())).toString());
    	
    	textFieldTestEnd.setText(data.getEndTime());
		
        textFieldReceivedResponses.setText((new Long(data.getNumberOfResponses())).toString());      
        textFieldMaxHeapUsage.setText((new Long(data.getMaxHeapSize())).toString());
        textFieldNumberOfRetries.setText((new Long(data.getNumberOfRetries())).toString()); 

        formatter = new Formatter();
		textFieldAvgRTT.setText(formatter.format( "%.2f", (new Double(data.getAvgRTT())) ).toString());          
		formatter = new Formatter();
		textFieldAvgServerTime.setText(formatter.format( "%.2f", (new Double(data.getAvgServerTime())) ).toString());       
		formatter = new Formatter();
		textFieldMaxRTT.setText(formatter.format( "%.2f", (new Double(data.getMaxRTT())) ).toString());
		formatter = new Formatter();
        textFieldMinRTT.setText(formatter.format( "%.2f", (new Double(data.getMinRTT())) ).toString());
        formatter = new Formatter();
        textFieldAvgCpuUsage.setText(formatter.format( "%.2f", (new Double(data.getMaxCpuUsage() * 100))).toString());
               
        textFieldSentEventMessages.setText((new Long(data.getNumberOfSentEventMessages())).toString());         
        textFieldReceivedConfirmEvents.setText((new Long(data.getNumberOfReceivedConfirmEvents())).toString());          
        textFieldLostConfirmEvents.setText((new Long(data.getNumberOfLostConfirmEvents())).toString());      
        textFieldRetriedEvents.setText((new Long(data.getNumberOfRetriedEvents())).toString());  
        
        // Aktualisieren des Frames auf dem Bildschirm
        frameBenchmarkingGui.update(frameBenchmarkingGui.getGraphics());
    }

    @Override
    public synchronized void setMessageLine(String message) {
        messageArea.append(message + "\n");
        messageArea.update(messageArea.getGraphics());
    }

    @Override
    public synchronized void resetCurrentRunTime() {
        timeCounter = 0;
        String strTimeCounter = (new Long(timeCounter)).toString();
        textFieldTestDuration.setText(strTimeCounter);

        // Aktualisieren des Ausgabefeldes auf dem Bildschirm
        textFieldTestDuration.update(textFieldTestDuration.getGraphics());
    }

    @Override
    public synchronized void addCurrentRunTime(long sec) {
        timeCounter += sec;
        String strTimeCounter = (new Long(timeCounter)).toString();
        textFieldTestDuration.setText(strTimeCounter);

        // Aktualisieren des Ausgabefeldes auf dem Bildschirm
        textFieldTestDuration.update(textFieldTestDuration.getGraphics());
    }

    private Component createSeparator(String text) {
        return DefaultComponentFactory.getInstance().createSeparator(text);
    }
    
    public static void main(String[] args) {

        PropertyConfigurator.configureAndWatch("log4j.client.properties", 60 * 1000);

        try {
            //UIManager.setLookAndFeel("com.jgoodies.plaf.plastic.PlasticXPLookAndFeel");
			UIManager.setLookAndFeel(new NimbusLookAndFeel());
        } catch (Exception e) {
            // Likely PlasticXP is not in the class path; ignore.
        }

        frameBenchmarkingGui = new JFrame("Benchmarking Client GUI");
        frameBenchmarkingGui.setTitle("Benchmark");
        frameBenchmarkingGui.add(new BenchmarkingClientGui());
        frameBenchmarkingGui.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        JComponent panel = new BenchmarkingClientGui().buildPanel();
        frameBenchmarkingGui.getContentPane().add(panel);
        frameBenchmarkingGui.pack();
        frameBenchmarkingGui.setVisible(true);
    }
}