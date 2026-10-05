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

# userActivity MV, grain (user, date, group). Org scope comes from the JWT.
# If your existing extract/ module passes extra args (pagination, filters), mirror them here.
USER_ACTIVITY_QUERY = """
{{
  userActivity(startDate: "{start}", endDate: "{end}") {{
    id
    orgId
    userId
    groupId
    groupName
    date
    uniqueAppsUsed
    totalNavigations
    activeDays
    username
    givenName
    familyName
  }}
}}
"""
