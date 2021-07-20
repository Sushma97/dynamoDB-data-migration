
'''
Wrapper class to run migration and updation together
'''

import argparse, sys
import properties as prp
import regionUpdation as rgUp
import migrationRegion as migUp

parser=argparse.ArgumentParser()

parser.add_argument('--env', help='enter the table name to be updated')
parser.add_argument('--puid', help='enter the puid to be updated (can be given as comma seperated value ex: --puid=1,2,3,4)')
parser.add_argument('--region',help = 'insert the region or list of regions to be inserted (can be given as comma seperated value ex: --region=EU,US)')

args = parser.parse_args()


prp.tables[0]['value'] = [("PARTNER#" + i) for i in args.puid.split(',')]
prp.tables[1]['value'] = list(map(int, args.puid.split(',')))
migUp.run()

print("Beginning the region updation for dynamoDB")
puids = args.puid.split(',')
rgUp.regionAwsTablUpdate(args.env,puids,args.region)
rgUp.regionUpdation(args.env,puids,args.region)
rgUp.regionUpdateForUserDetails(args.env,puids,args.region)

print("Done Updating the region")
