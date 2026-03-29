// lib/mailbox.js
// Connects to the dedicated mailbox via IMAP, finds unread emails
// with Excel attachments matching the subject filter, returns buffers

export async function fetchLatestAttachment() {
  const { ImapFlow } = await import('imapflow')

  const client = new ImapFlow({
    host:   process.env.MAIL_HOST,
    port:   parseInt(process.env.MAIL_PORT || '993'),
    secure: true,
    auth: {
      user: process.env.MAIL_USER,
      pass: process.env.MAIL_PASSWORD,
    },
    logger: false,
  })

  await client.connect()

  let result = null

  try {
    await client.mailboxOpen('INBOX')
    const subjectFilter = process.env.MAIL_SUBJECT_FILTER || 'Dispatch Report'

    // Search for unread emails with our subject filter
    const uids = await client.search({
      unseen: true,
      subject: subjectFilter,
    })

    if (!uids.length) {
      console.log('No new dispatch emails found')
      return null
    }

    // Get the most recent matching email
    const latestUid = uids[uids.length - 1]

    for await (const msg of client.fetch([latestUid], { bodyStructure: true, envelope: true })) {
      const parts = flattenParts(msg.bodyStructure)
      const xlsPart = parts.find(p =>
        p.type === 'application' &&
        (p.subtype?.includes('sheet') ||
         p.subtype?.includes('excel') ||
         p.subtype?.includes('vnd.ms') ||
         p.parameters?.name?.match(/\.xlsx?$/i))
      )

      if (!xlsPart) continue

      // Fetch the attachment content
      const { content } = await client.download(latestUid, xlsPart.part)
      const chunks = []
      for await (const chunk of content) chunks.push(chunk)
      const buffer = Buffer.concat(chunks)

      // Mark as read
      await client.messageFlagsAdd([latestUid], ['\\Seen'])

      result = {
        buffer,
        filename: xlsPart.parameters?.name || 'dispatch.xlsx',
        subject:  msg.envelope?.subject || '',
        date:     msg.envelope?.date || new Date(),
      }
      break
    }
  } finally {
    await client.logout()
  }

  return result
}

function flattenParts(structure, partNum = '') {
  const parts = []
  if (!structure) return parts

  if (structure.childNodes?.length) {
    structure.childNodes.forEach((child, i) => {
      const num = partNum ? `${partNum}.${i + 1}` : `${i + 1}`
      parts.push(...flattenParts(child, num))
    })
  } else {
    parts.push({ ...structure, part: partNum || '1' })
  }
  return parts
}
