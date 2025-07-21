from mega import mega
mega = mega()
m = mega.login("your_email", "your_password")
file = m.upload("example.pdf")
public_url = m.get_link(file)