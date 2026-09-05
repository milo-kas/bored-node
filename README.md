# bored-node
> A device that has nothing better to do than to become a clipboard

## What it actually does
`bored-node` essentially turns your device into a LAN peer-to-peer clipboard. 
Paste notes or files from one device, and it appears on every other device running the app.

P2P; No server; No cloud; No accounts. **_Simply your WiFi network._**

### Features
- **Send text:** type/paste a message on your phone, see it on your laptop.
- **Send files:** drag and drop a file on your laptop, see it on your phone (up to 100 MB).
- **Local queue:** items arrive in a device's local queue. Tap to copy, swipe to dismiss or star to keep.
- **Auto-discovery:** devices on the same network and app discover each other via mDNS.
- **Ephemeral:** nothing is stored unless you star it. Unstar = Delete.


## Security Note
- Current design assumes a trusted home network (no DoS protection for public WiFi). 
Moreover, anyone on the same network can send unsolicited content.