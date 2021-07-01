src_region = 'us-west-2'
dest_region = 'eu-central-1'
tables = [{'src_table': 'cxpxactivitylogs',
'dst_table': 'cxpp-activity-logs-test',
'value' : ['PARTNER#1719376510'],
'key' : 'PK'},
{'src_table': 'cxpp_portal_feedback',
'dst_table': 'cxpp_portal_feedback',
'value' : [1452997235],
'key' : 'puid'}
]