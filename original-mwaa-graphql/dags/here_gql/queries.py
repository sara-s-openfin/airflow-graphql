# orgSummary: non-windowed, returns a list, tiny payload -> ideal connectivity check.
SMOKE_QUERY = """
{
  orgSummary {
    orgId
    totalUsers
    totalAppsUsed
    lastActivity
  }
}
"""
