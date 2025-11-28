import re
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest


async def join_chat(client, target: str):
    """Join a chat/channel using @username or invite link.
    Supports: @username, t.me/username, https://t.me/+inviteHash, t.me/joinchat/inviteHash
    """
    t = target.strip()
    try:
        # private invite link
        m = re.search(r"t\.me/(?:\+|joinchat/)([A-Za-z0-9_-]+)", t)
        if m:
            invite_hash = m.group(1)
            return await client(ImportChatInviteRequest(invite_hash))
        # username
        if t.startswith('@'):
            entity = await client.get_entity(t)
            return await client(JoinChannelRequest(entity))
        # t.me/username
        m2 = re.search(r"t\.me/([A-Za-z0-9_]+)$", t)
        if m2:
            username = '@' + m2.group(1)
            entity = await client.get_entity(username)
            return await client(JoinChannelRequest(entity))
        # fallback: treat as username
        entity = await client.get_entity(t)
        return await client(JoinChannelRequest(entity))
    except Exception as e:
        raise RuntimeError(str(e))
