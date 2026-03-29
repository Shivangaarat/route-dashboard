// app/layout.jsx
export const metadata = { title: 'Route Pattern Intelligence Dashboard' }
export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body style={{ margin: 0, background: '#eeedea', minHeight: '100vh' }}>
        {children}
      </body>
    </html>
  )
}
