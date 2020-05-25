#!/usr/bin/python3

import os
import sys
import time
import re
import mysql.connector
from dbfread import DBF

#Creating/Opening Log Log File
log = open('INS.log','a')
print('Log file Mode: ', log.mode)

#Appending Log
log.write('\n')
now = time.asctime(time.localtime(time.time()))
l = 0
while l != 19:
	if (l == 9):
		log.write('\n')
		log.write('##### ')
		log.write(str(now))
		log.write(' ##### \n')
	else:
		log.write('####')

	l += 1

log.write('\n')

#Reading DBF file / Changing to DBF Directory
os.chdir('/reports/data/TAM')
table = DBF('INS.DBF')

#Opening Database conn
db = mysql.connector.connect(host='localhost',
                             user='root',
                             password='18RRglMN!!#',
                             database='TAM')

cursor = db.cursor()
cursor.execute('SELECT VERSION()')
data = cursor.fetchone()

#Creating a Date conversion function
def con_date(d):
	if d is 'A':
		return '200'
	elif d is'B':
		return '201'
	elif d is 'C':
		return '202'
	else:
		return ''
# end of Con_date

name_table =''


# Info of Records and Database
log.write('SQL Database version : %s'% data)
log.write('\n')

length = len(table)
log.write(' Records: ')
log.write(str(length))

n = 0



#Sepearting DBF Data OUT
for records in table:
		c = list()
		
		n += 1
		s = list(records.values())
		
		k = s[0]
		
		if k is 'C':
			name_table = 'Customer'
			
			str = s[15]
			print(len(str))
			
			#Seperating C into apropriate fields
			aa = str[:2]
			ab = str[2:4]
			ac = str[4:19]
			ad = str[19:39]
			ae = str[39:56]
			af = str[62:74]
			ag = str[75:78]
			ah = str[84:91]
			ai = str[91:92]
			
			#Convert d1 date to Date format
			aj = str[92:100]
			ba = con_date(aj[6:7])
			aj = aj.replace(aj[6:7],ba)
			
			#Convert d2 date to Date format
			ak = str[100:108]
			ba = con_date(ak[6:7])
			ak = ak.replace(ak[6:7],ba)
			
			#Continue Seperate
			al = str[108:111]
			am = str[111:114]
			an = str[114:144]
			ao = str[144:174]
			ap = str[174:185]
			aq = str[185:196]
			ar = str[199:200]
			at = str[200:206]
			au = str[208:211]
			
			# Convert PayDate year to proper format
			av = str[211:217]
			bb = con_date(av[:1])
			av = av.replace(av[:1],bb)
			
			# Continue to Seperate
			aw = str[226:228]
			ax = str[228:241]
			
			c = [aa.strip(),ab.strip(),ac.strip(),ad.strip(),ae.strip(),af.strip(),ag.strip(),ah.strip(),ai.strip(),aj.strip(),ak.strip(),al.strip(),am.strip(),
				an.strip(),ao.strip(),ap.strip(),aq.strip(),ar.strip(),at.strip(),au.strip(),av.strip(),aw.strip(),ax.strip()]
			
			data_customer = (s[0],s[1],)
			
			
			print(s[2])
			print("\n")
			print(*c, sep = ",")
			print("\n")
			
			
			
		input("Press Enter to continue...")
		#End of if C for customer

# add in the first 14 data items
c_table = ("Insert into %(name_table)s"
			"(rec,agcy,brch,name,street,city,st,zip,phr,phb,info_n,follow_n,note_n,)"
			"VALUES (%(s[1])s,%(s[2])s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
	
#Closing Database and DBF
db.close()


#Closing Log
now = time.asctime(time.localtime(time.time()))
log.write('\n')
log.write('CLOSING....')
log.write('\n')
c = 0
while c != 19:
	if (c == 9): 
		log.write('\n')
		log.write('##### ')
		log.write(str(now))
		log.write(' ##### \n')
	else:
		log.write('####')

	c += 1

log.write('\n')

