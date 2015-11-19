package edu.hm.dako.chat.client;

import java.awt.BorderLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.text.NumberFormat;
import java.util.Vector;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFormattedTextField;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;
import javax.swing.border.EmptyBorder;
import javax.swing.plaf.nimbus.NimbusLookAndFeel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

import edu.hm.dako.chat.common.ExceptionHandler;
import edu.hm.dako.chat.tcp.TcpChatSimpleClientImpl;

/**
 * Chat-Client GUI
 *
 * @author Bjoern Rottmueller
 */
public class ChatClientSwingGUI extends JFrame implements ChatClientUserInterface {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static Log log = LogFactory.getLog(ChatClientSwingGUI.class);

	// Elements for the Login Screen
	private JPanel panLoginScreen;

	private JLabel lblUserName;

	private JTextField txtUserName;

	private JLabel lblServerNameOrIp;

	private JTextField txtServerNameOrIp;

	private JLabel lblServerPort;

	private JFormattedTextField txtServerPort;

	private JButton btnLogin;

	private JButton btnExit;

	// Elements for the Chat Screen
	private JPanel panChatScreen;

	private JLabel lblChatUserList;

	private JTextArea txtAreaChatUserList;

	private JScrollPane scrPaneChatUserList;

	private JLabel lblChatConversation;

	private JTextArea txtAreaChatConversation;

	private JScrollPane scrPaneChatConversation;

	private JLabel lblChatMessage;

	private JTextField txtChatMessage;

	private JButton btnSubmit;

	private JButton btnLogout;

	private String userName;

	private String serverNameOrIp;

	private String serverPort;

	private Integer intServerPort;

	private TcpChatSimpleClientImpl communicator;

	private AboutDialog aboutDialog;

	@Override
	public synchronized void logoutComplete() {
		// TODO: Exit after Response received
		log.debug("Logout vollstaendig erfolgt, Anwendung schliessen");
		System.exit(0);
	}

	@Override
	public synchronized void loginComplete() {
		setContentPane(panChatScreen);
		setTitle("Client fuer Chat-Application");
		pack();

		setVisible(true);

		txtChatMessage.requestFocus();
	}

	@Override
	public synchronized void setBlock(boolean block) {
		if (block) {
			btnSubmit.setEnabled(!block);
		}
		else {
			btnSubmit.setEnabled(!block);
		}
	}

	@Override
	public synchronized void setErrorMessage(String sender, String errorMessage, long errorCode) {
		// TODO: modalen Dialog mit OK Button
		aboutDialog = new AboutDialog(this, makeHtmlString(makeErrorMessage(sender, errorMessage)));
		aboutDialog.setVisible(true);
		this.txtUserName.setText("");
	}

	private String makeHtmlString(String s) {
		StringBuilder tempStringBuilder = new StringBuilder();
		tempStringBuilder.append("<html>");
		tempStringBuilder.append(s);
		tempStringBuilder.append("</html>");
		String tempResultString = tempStringBuilder.toString();
		String resultWithBr = tempResultString.replace("\n", "<br>");
		return resultWithBr;
	}

	private String makeErrorMessage(String sender, String errorMessage) {
		StringBuilder completeErrorMessage = new StringBuilder();
		completeErrorMessage.append("Meldung von ");
		completeErrorMessage.append(sender);
		completeErrorMessage.append(" erhalten!");
		completeErrorMessage.append("\n\n");
		completeErrorMessage.append(errorMessage);
		return completeErrorMessage.toString();
	}

	@Override
	public synchronized void setUserList(Vector<String> userList) {
		fillUserList(userList);
	}

	@Override
	public synchronized void setMessageLine(String sender, String message) {
		String messageText = "<" + sender + ">: " + message;
		txtAreaChatConversation.append(messageText + "\n");
		txtAreaChatConversation.setCaretPosition(txtAreaChatConversation.getDocument().getLength());
	}

	public ChatClientSwingGUI() {
		declareJComponents();

		declareButtonListener();

		declareWindowListener();

		declareLocalVariables();

		setDefaultValues();

		makeLoginScreen();

		makeChatScreen();

		setDefaultComponentsValues();

		setContentPane(panLoginScreen);

		setResizable(false);
		setTitle("Chat-Application --- Login");
		setLocation(100, 100);
		setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
		pack();
	}

	private void declareWindowListener() {
		this.addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosing(WindowEvent e) {
				performLogout();
			}
		});
	}

	private void setDefaultComponentsValues() {
		this.txtServerNameOrIp.setText(this.serverNameOrIp);
		this.txtServerPort.setText(this.serverPort);
	}

	private void setDefaultValues() {
		this.serverNameOrIp = "127.0.0.1";
		this.serverPort = "50000";
	}

	private void declareLocalVariables() {
		this.serverNameOrIp = null;
		this.serverPort = null;
		this.intServerPort = Integer.valueOf(-1);
	}

	private void fillUserList(Vector<String> names) {
		log.debug("Userliste in GUI: " + names);
		log.debug("Groesse des Vektors: " + names.size());

		txtAreaChatUserList.setText("");

		for (int i = 0; i < names.size(); i++) {
			if (names.get(i).equals(userName))
				txtAreaChatUserList.append("<" + names.get(i) + ">\n");
			else
				txtAreaChatUserList.append(names.get(i) + "\n");
		}
	}

	private void declareButtonListener() {
		btnLogin.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				performLogin();
			}
		});

		btnExit.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				performLogout();
			}
		});

		btnLogout.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				performLogout();
			}
		});

		btnSubmit.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				submitChatMessage();
			}
		});

		txtUserName.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				performLogin();
			}
		});

		txtServerNameOrIp.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				performLogin();
			}
		});

		txtServerPort.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				performLogin();
			}
		});

		txtChatMessage.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				submitChatMessage();
			}
		});
	}

	private void performLogout() {
		try {
			communicator.logout(userName);
		} catch (Exception e2) {
			ExceptionHandler.logException(e2);
		}

		log.debug("Chat-Client " + userName + " ordnungsgemaess abgemeldet");
	}

	private void performLogin() {
		// Validierung fuer Serverport
		this.serverPort = txtServerPort.getText();
		if (serverPort.matches("[0-9]+")) {
			intServerPort = new Integer(serverPort);
			if ((intServerPort.intValue() < 1) || (intServerPort.intValue() > 65535)) {
				// nicht im Wertebereich
				log.debug("Serverport nicht im Bereich von 1 bis 65535 angegeben");		
				System.exit(9);
			} else {
				log.debug("Serverport: " + intServerPort);
				serverNameOrIp = txtServerNameOrIp.getText();
				log.debug("Serveradresse: " + serverNameOrIp);				
				communicator = new TcpChatSimpleClientImpl(this, intServerPort.intValue(), serverNameOrIp);
			}
		} else {
			System.exit(9);
		}

		userName = txtUserName.getText();
		try {
			communicator.login(userName);
		} catch (Exception e2) {
			ExceptionHandler.logException(e2);
		}
	}

	private void submitChatMessage() {
		// Eingegebene Chat-Nachricht an Server senden
		communicator.tell(userName, txtChatMessage.getText());
		txtChatMessage.setText("");
	}

	private void makeChatScreen() {
		panChatScreen.setLayout(new BorderLayout(3, 3));

		JPanel centerPane = new JPanel();
		centerPane.setLayout(new BoxLayout(centerPane, BoxLayout.PAGE_AXIS));

		JPanel panChatUser = new JPanel();
		panChatUser.setLayout(new BorderLayout());
		panChatUser.setBorder(new EmptyBorder(2, 2, 2, 2));
		panChatUser.add(lblChatUserList, BorderLayout.NORTH);
		panChatUser.add(scrPaneChatUserList, BorderLayout.CENTER);
		centerPane.add(panChatUser);

		JPanel panChatConversation = new JPanel();
		panChatConversation.setLayout(new BorderLayout());
		panChatConversation.setBorder(new EmptyBorder(2, 2, 2, 2));
		panChatConversation.add(lblChatConversation, BorderLayout.NORTH);
		panChatConversation.add(scrPaneChatConversation, BorderLayout.CENTER);
		centerPane.add(panChatConversation);

		JPanel panMessage = new JPanel();
		panMessage.setLayout(new GridLayout(2, 1));
		panMessage.setBorder(new EmptyBorder(2, 2, 2, 2));
		panMessage.add(lblChatMessage);
		panMessage.add(txtChatMessage);
		centerPane.add(panMessage);

		JPanel panLoginButtons = new JPanel();
		panLoginButtons.setLayout(new GridLayout(1, 2));
		panLoginButtons.setBorder(new EmptyBorder(2, 2, 2, 2));
		panLoginButtons.add(btnSubmit);
		panLoginButtons.add(btnLogout);
		centerPane.add(panLoginButtons);

		panChatScreen.add(centerPane, BorderLayout.CENTER);
		panChatScreen.add(panLoginButtons, BorderLayout.SOUTH);
		panChatScreen.setBorder(new EmptyBorder(5, 5, 5, 5));
	}

	private void makeLoginScreen() {
		panLoginScreen.setLayout(new BorderLayout(3, 3));

		JPanel centerPane = new JPanel();
		centerPane.setLayout(new BoxLayout(centerPane, BoxLayout.PAGE_AXIS));

		JPanel panUsername = new JPanel();
		panUsername.setLayout(new GridLayout(2, 1));
		panUsername.setBorder(new EmptyBorder(2, 2, 2, 2));
		panUsername.add(lblUserName);
		panUsername.add(txtUserName);
		centerPane.add(panUsername);

		JPanel panServerNameOrIp = new JPanel();
		panServerNameOrIp.setLayout(new GridLayout(2, 1));
		panServerNameOrIp.setBorder(new EmptyBorder(2, 2, 2, 2));
		panServerNameOrIp.add(lblServerNameOrIp);
		panServerNameOrIp.add(txtServerNameOrIp);
		centerPane.add(panServerNameOrIp);

		JPanel panServerPort = new JPanel();
		panServerPort.setLayout(new GridLayout(2, 1));
		panServerPort.setBorder(new EmptyBorder(2, 2, 2, 2));
		panServerPort.add(lblServerPort);
		panServerPort.add(txtServerPort);
		centerPane.add(panServerPort);

		JPanel panLoginButtons = new JPanel();
		panLoginButtons.setLayout(new GridLayout(1, 2));
		panLoginButtons.setBorder(new EmptyBorder(2, 2, 2, 2));
		panLoginButtons.add(btnLogin);
		panLoginButtons.add(btnExit);
		centerPane.add(panLoginButtons);

		panLoginScreen.add(centerPane, BorderLayout.CENTER);
		panLoginScreen.add(panLoginButtons, BorderLayout.SOUTH);
		panLoginScreen.setBorder(new EmptyBorder(5, 5, 5, 5));
	}

	private void declareJComponents() {
		panLoginScreen = new JPanel();
		lblUserName = new JLabel("Username:");
		txtUserName = new JTextField(30);
		lblServerNameOrIp = new JLabel("Servername or IP:");
		txtServerNameOrIp = new JTextField(30);
		lblServerPort = new JLabel("Server Port:");
		NumberFormat portFormat = NumberFormat.getIntegerInstance();
		txtServerPort = new JFormattedTextField(portFormat);
		btnLogin = new JButton("Login");
		btnExit = new JButton("Exit");

		panChatScreen = new JPanel();
		lblChatUserList = new JLabel("Chat User List:");
		txtAreaChatUserList = new JTextArea(10, 50);
		txtAreaChatUserList.setEditable(false);
		scrPaneChatUserList = new JScrollPane(txtAreaChatUserList);
		lblChatConversation = new JLabel("Chat Conversation:");
		txtAreaChatConversation = new JTextArea(10, 50);
		txtAreaChatConversation.setEditable(false);
		scrPaneChatConversation = new JScrollPane(txtAreaChatConversation);
		lblChatMessage = new JLabel("Message to send:");
		txtChatMessage = new JTextField(50);
		btnSubmit = new JButton("Submit");
		btnLogout = new JButton("Logout");
	}

	public static void main(String args[]) {
		PropertyConfigurator.configureAndWatch("log4j.client.properties", 60 * 1000);

		SwingUtilities.invokeLater(new Runnable() {
			@Override
			public void run() {
				try {
					UIManager.setLookAndFeel(new NimbusLookAndFeel());
				} catch (UnsupportedLookAndFeelException e) {
					e.printStackTrace();
				}

//				UIManager.put("swing.boldMetal", Boolean.FALSE);

				new ChatClientSwingGUI().setVisible(true);
			}
		});
	}
}

class AboutDialog extends JDialog {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public AboutDialog(JFrame owner, String completeErrorMessage) {
		super(owner, "About DialogTest", true);

		// add HTML label to center
		JLabel errorMessage = new JLabel(completeErrorMessage);
		errorMessage.setBorder(new EmptyBorder(5, 5, 5, 5 ));
		add(errorMessage, BorderLayout.CENTER);

		// Ok button closes the dialog
		JButton ok = new JButton("Ok");
		ok.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				setVisible(false);
			}
		});

		// add Ok button to southern border
		JPanel panel = new JPanel();
		panel.setBorder(new EmptyBorder(5, 5, 5, 5));
		panel.add(ok);
		add(panel, BorderLayout.SOUTH);

		setLocation(100, 100);
		setSize(500, 250);
	}
}