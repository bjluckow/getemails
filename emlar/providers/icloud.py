from __future__ import annotations
 
from emlar.providers.imap import IMAPProvider
 
 
class iCloudProvider(IMAPProvider):
    HOST = "imap.mail.me.com"
    PORT = 993
 