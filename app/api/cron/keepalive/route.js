// app/api/cron/keepalive/route.js
// Runs weekends to keep Neon database active
// Also runs weekly auto-backup on Sundays

import { neon } from '@neondatabase/serverless'

export const runtime = 'nodejs'

function db() { return neon(process.env.DATABASE_URL) }

export async function GET(request) {
  const auth = request.headers.get('authorization')
  if (auth !== `Bearer ${process.env.CRON_SECRET}`) {
    return Response.json({ error: 'Unauthorized' }, { status: 401 })
  }

  try {
    const sql = db()

    // Simple keepalive ping
    const ping = await sql`SELECT COUNT(*) as total FROM raw_tasks`
    const total = ping[0]?.total || 0

    // Check if today is Sunday for auto-backup
    const today = new Date()
    const isSunday = today.getDay() === 0
    const autoBackupEnabled = process.env.AUTO_BACKUP_ENABLED !== 'false'

    let backupStatus = 'skipped'
    if (isSunday && autoBackupEnabled) {
      // Log backup attempt
      await sql`
        INSERT INTO database_backups (backup_type, triggered_by, status, raw_task_count)
        VALUES ('auto-weekly', 'cron-keepalive', 'logged', ${parseInt(total)})
      `
      backupStatus = 'logged'
    }

    return Response.json({
      status:        'alive',
      total_tasks:   total,
      checked_at:    new Date().toISOString(),
      backup_status: backupStatus,
    })

  } catch (err) {
    return Response.json({ error: err.message }, { status: 500 })
  }
}
