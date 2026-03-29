// app/api/cron/check-mailbox/route.js
// Called by Vercel Cron at 6am Mon-Fri
// Checks the dedicated mailbox for new dispatch Excel attachments

import { fetchLatestAttachment } from '../../../../lib/mailbox.js'
import { parseExcelBuffer } from '../../../../lib/engine.js'
import { saveSnapshot } from '../../../../lib/db.js'

export const runtime = 'nodejs'
export const maxDuration = 60

export async function GET(request) {
  // Verify this is a legitimate Vercel cron call
  const authHeader = request.headers.get('authorization')
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return Response.json({ error: 'Unauthorized' }, { status: 401 })
  }

  try {
    console.log('[cron] Checking mailbox for dispatch reports...')
    const attachment = await fetchLatestAttachment()

    if (!attachment) {
      return Response.json({
        status: 'no_mail',
        message: 'No new dispatch emails found',
        checkedAt: new Date().toISOString()
      })
    }

    console.log(`[cron] Found attachment: ${attachment.filename}`)
    const rows = parseExcelBuffer(attachment.buffer)

    if (!rows.length) {
      return Response.json({ status: 'empty', message: 'Attachment parsed but contained no valid rows' })
    }

    // Use the email date as the import date if available, else today
    const importDate = attachment.date
      ? new Date(attachment.date).toISOString().split('T')[0]
      : new Date().toISOString().split('T')[0]

    const result = await saveSnapshot({ rows, source: 'auto', importDate })

    console.log(`[cron] Saved ${result.rowCount} rows for ${result.date}`)
    return Response.json({
      status: 'success',
      ...result,
      filename: attachment.filename,
      emailSubject: attachment.subject,
    })
  } catch (err) {
    console.error('[cron] Error:', err)
    return Response.json({ error: err.message }, { status: 500 })
  }
}
