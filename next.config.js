/** @type {import('next').NextConfig} */
const nextConfig = {
  serverExternalPackages: ['imapflow', 'xlsx', 'exceljs'],
  experimental: {
    serverActions: {
      bodySizeLimit: '50mb',
    },
  },
}
module.exports = nextConfig
