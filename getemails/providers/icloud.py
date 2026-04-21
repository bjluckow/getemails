from __future__ import annotations
 
from getemails.providers.base import AccountConfig
from getemails.providers.imap import IMAPProvider
 
 
class iCloudProvider(IMAPProvider):
    HOST = "imap.mail.me.com"
    PORT = 993
 