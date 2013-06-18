package com.xingcloud.util.manager;


import com.xingcloud.util.config.ConfigReader;

import javax.mail.*;
import javax.mail.internet.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MailManager {
    static class MailConstants {
        /*Mail*/
        public static String HOST = "smtp.qq.com";
        public static String EMAIL_FROM = "EMAIL_FROM";
        public static String EMAIL_TO = "EMAIL_TO";
        public static String EMAIL_SUBJECT = "EMAIL_SUBJECT";
        public static String EMAIL_TEXT = "EMAIL_TEXT";
        public static String USER_NAME = "USER_NAME";
        public static String PASSWORD = "PASSWORD";
    }


    private static MailManager m_instance = null;
    
    private Map<String, String> data = new HashMap<String, String>();
    private static String username;
    private static String password;
    private static String from;
    
    private MailManager() {
        initData();
    }
    
    public synchronized static MailManager getInstance() {
        if(m_instance == null) {
            m_instance = new MailManager();
        }
        return m_instance;
    }
    
    public void setSubject(String subject) {
        data.put(MailConstants.EMAIL_SUBJECT, subject);
    }
    
    public void setContent(String message) {
        data.put(MailConstants.EMAIL_TEXT, message);
    }
    
    private void initData() {
        from = getMailInfo("mail_from");
        
        String mailTo = getMailInfo("mail_to");
        data.put(MailConstants.EMAIL_TO, mailTo);

        username = getMailInfo("username");
        password = getMailInfo("password");
    }
    
    public void setMailTo(String address) {
        data.put(MailConstants.EMAIL_TO, address);
    }
    
    private String getMailInfo(String field) {
        return ConfigReader.getConfig("Config.xml", "mail", field);
    }
    
    public boolean sendEmail(String subject, String content) {
        setSubject(subject);
        setContent(content);
        return sendEmail();
    }
    
    public boolean sendEmail(String subject, String content, Exception e) {
        setSubject(subject);
        setContent(content + "\n" + e.getCause().getMessage());
        return sendEmail();
    }
    
    public boolean sendEmail() {
        // 创建Properties 对象
        Properties props = System.getProperties();
        props.put("mail.smtp.host", MailConstants.HOST); // 全局变量
        props.put("mail.smtp.auth", "true");
     
        // 创建邮件会话
        Session session = Session.getDefaultInstance(props,
        new Authenticator() { // 验账账户
            @Override
            public PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username,
                                                  password);
            }
        });
     
        try {
            // 定义邮件信息
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(from));
            
            String[] mailToList = data.get(MailConstants.EMAIL_TO).split(",");
            
            for(String mailTo : mailToList) {
                message.addRecipient(
                        Message.RecipientType.TO,
                        new InternetAddress(
                            mailTo
                        )
                    );
            }

            // 要编码，否则中文会出乱码，貌似这个方法是对数据进行了
            //("=?GB2312?B?"+enc.encode(subject.getBytes())+"?=")形势的包装
            message.setSubject(MimeUtility.encodeText(data.get(MailConstants.EMAIL_SUBJECT), "gbk", "B"));
     
            MimeMultipart mmp = new MimeMultipart();
            MimeBodyPart mbp_text = new MimeBodyPart();
            // "text/plain"是文本型，没有样式，
            //"text/html"是html样式，可以解析html标签
            mbp_text.setContent(data.get(MailConstants.EMAIL_TEXT),
                                "text/plain;charset=gbk");
            mmp.addBodyPart(mbp_text); // 加入邮件正文
     
            message.setContent(mmp);
            // message.setText(data.get(MailConstants.EMAIL_TEXT));
     
            // 发送消息
            // session.getTransport("smtp").send(message); //也可以这样创建Transport对象
            Transport.send(message);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    
    public static void main(String[] args) {
        MailManager.getInstance().sendEmail("Test", "Ha");
    }
    
}
