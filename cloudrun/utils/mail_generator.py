import smtplib

def send_mail(mail, message):
    # list of email_id to send the mail
    li = [mail]
    print(li)

    for dest in li:
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.starttls()
        s.login("pasumarthipradeepshiva@gmail.com", "qxbkzstssbxojlto")
        s.sendmail("pasumarthipradeepshiva@gmail.com", dest, message)
        s.quit()

