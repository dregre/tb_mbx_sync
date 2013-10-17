'''
Thunderbird Mail Synchronizer v. 1.0
Syncs mail across two profiles, in the mailboxes that are present in
both profiles.

Copyright (C) 2013 Andre Gregori

Licensed under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License.  You may obtain
a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

'''


import fileinput, os, hashlib, sys, multiprocessing, re, time
from subprocess import call, Popen, PIPE
import sqlite3 as lite

'''
Purges inactive memory on Macs if free memory < 500MB and inactive
memory > 1GB.  Runs check every 10 seconds as a daemon (terminating when
main program terminates). 
'''
def mac_pages2mb(page_count):
    return int(page_count) * 4096 / 1024 ** 2

def mac_free_inactive():
    vmstat = Popen('vm_stat', shell=True, stdout=PIPE).stdout.read()
    inactive = mac_pages2mb(RE_INACTIVE.search(vmstat).group(1))
    free = mac_pages2mb(RE_FREE.search(vmstat).group(1)) + \
            mac_pages2mb(RE_SPECULATIVE.search(vmstat).group(1))
    return free, inactive

def mac_purge():
    while(True):
        time.sleep(10)
        free, inactive = mac_free_inactive()
        if (free < FREE_THRESHOLD) and (inactive > INACTIVE_THRESHOLD):
            print("Free: %dmb < %dmb" % (free, FREE_THRESHOLD))
            print("Inactive: %dmb > %dmb" % (inactive, INACTIVE_THRESHOLD))
            print('Purging...')
            call('/usr/bin/purge', shell=True)
       
if sys.platform == "darwin":
    INACTIVE_THRESHOLD = 1024  # Number of MBs
    FREE_THRESHOLD = INACTIVE_THRESHOLD / 2
    RE_INACTIVE = re.compile('Pages inactive:\s+(\d+)')
    RE_FREE = re.compile('Pages free:\s+(\d+)')
    RE_SPECULATIVE = re.compile('Pages speculative:\s+(\d+)')
    #LOCK_FILE = '/var/tmp/releasemem.lock'
    
    p = multiprocessing.Process(target=mac_purge)
    p.daemon = True
    p.start()

'''
Inserts data from two corresponding popstate.dat files into database.
'''
def insert_popstates(filepaths, cur):
    for index, filepath in enumerate(filepaths):
        print "Reading: " + filepath
        with con:
            cur.execute("CREATE TABLE Pop%i(id INTEGER PRIMARY KEY,"
                        " msgid TEXT, poptag INT);" % index)
        
            for i, line in enumerate(fileinput.input(filepath)):
                if i > 5:
                    msgid = line.split(' ')[1]
                    cur.execute("INSERT INTO Pop%i(msgid, poptag) VALUES"
                                " ('%s', '%s');" % (index, msgid, line))

'''
Compares data from two corresponding popstate.dat files.  Appends each
popstate.dat file with unique values.   
'''                
def compare_popstates(filepaths, cur):
    for pop, _ in enumerate(filepaths):
        pop2 = not pop

        with con:
            cur.execute("SELECT poptag FROM Pop%i WHERE msgid NOT IN "
                        "(SELECT msgid FROM Pop%i)" % (pop, pop2))
            f = open(filepaths[pop2], 'a')
            u = 0
            while(True):      
                uniquepoptag = cur.fetchone()
                if not uniquepoptag: break
                f.write(uniquepoptag[0])
                u+=1
            if u > 0:
                print ("%i unique entries found in: %s.\nWritten to: %s"
                       % (u, filepaths[pop2], _))

'''
Returns paths to the mailboxes and popstate.dat files found under path.
(Filters out everything that is not a mailbox or a popstate.dat file).
'''
def sieve(path):
    filteredfiles = []
    for root, _, files in os.walk(path):
        for f in files:
            if f == 'popstate.dat' or f.find('.') < 0:
                filteredfiles.append(os.path.join(root, f))
    return filteredfiles

'''
Compares paths in path1 with the paths in path2.  Returns nonunique
paths as matchedfiles.  Returns unique paths in path1 and path2 as
uniquefiles1 and uniquefiles2, respectively.
'''
def compare_paths(path1, path2):
    uniquefiles1 = []
    uniquefiles2 = sieve(path2)
    matchedfiles = []
    p2files = sieve(path2)
    
    for filename1 in sieve(path1):
        filename2 = match_filename(filename1.replace(path1, ''), p2files,
                                   path2, uniquefiles2)
        if filename2:
            matchedfiles.append((filename1, filename2))
        else:
            uniquefiles1.append(filename1)
    return matchedfiles, uniquefiles1, uniquefiles2 

'''
This is a helper function for compare_paths above.  If a filename in
files matches basename, this function removes the filename from uniques
and returns it.  Otherwise, returns None.
'''
def match_filename(basename, files, path, uniques):
    for filename in files:
        if filename.replace(path, '') == basename:
            uniques.remove(filename)
            return filename
    return None

'''
Returns an e-mail message at the given offset from mailbox.   
'''
def msg(offset, mailbox):
    message = []
    
    f = open(mailbox, 'r')
    f.seek(offset)

    i = 0
    while(True):
        l = f.readline()
        if not l: break
        if l[:7].lower() == 'from - ' and i > 0:
            break
        message.append(l)
        i+=1
    f.close()
    return ''.join(message)

'''
Creates a hash for the body and certain header fields (from, to, cc,
subject) of each message in each of the provided mailboxes.  Adds hash
to database.
'''
def store_hashes(mailboxes, cur):
    for fileindex, filepath in enumerate(mailboxes):
        print 'Reading: %s' % filepath
        
        inheader = False
        message = []
        header = []
        offset = 0
        msgoffset = 0
        h = None
        
        with con:
            cur.execute("CREATE TABLE Mbx%i(id INTEGER PRIMARY KEY, hash TEXT,"
                        " offset INT);" % fileindex)
        
        for i, line in enumerate(fileinput.input(filepath)):
            line2 = line.rstrip()
                
            if not inheader and line2[:7].lower() == 'from - ':
                inheader = True  
                
                # The beginning of a new message marks the end of the previous
                # message.  Generates the hash for the previous message,
                # updates the database, and clears the message and header vars.      
                if i > 0:
                    h = hashlib.md5()
                    h.update('\n'.join(header))
                    h.update('\n'.join(message))                
                    with con:
                        cur.execute("INSERT INTO Mbx%i(hash, offset) VALUES "
                                    "('%s', %i);" % (fileindex,
                                                     h.hexdigest(),
                                                     msgoffset))
                
                    message = []
                    header = []
                    msgoffset = offset
                    
            elif inheader and (line2[:8].lower() == 'subject:' or
                               line2[:5].lower() == 'from:' or
                               line2[:3].lower() == "to:" or
                               line2[:3].lower() == "cc:"):
                header.append(line2)
                
            elif inheader and line2 == "":
                inheader = False
                
            elif not inheader:
                message.append(line2)
                
            offset += len(line)
            
        else:
            h = hashlib.md5()
            h.update('\n'.join(header))
            h.update('\n'.join(message))
            with con:
                cur.execute("INSERT INTO Mbx%i(hash, offset) VALUES"
                            " ('%s', %i);" % (fileindex,
                                              h.hexdigest(),
                                              msgoffset))
            
'''
Compares the hash tables of mailboxes.  Appends messages keyed to unique
hashes to deficient mailbox.     
'''
def comparemsgs(filepaths, cur):
    for fileindex, filepath in enumerate(filepaths):
        otherindex = not fileindex
        
        print ('Looking for messages in: %s\nthat are not in: %s' %
               (filepaths[otherindex], filepath))
        
        f = open(filepath, 'a')    
        with con:
            cur.execute("SELECT offset FROM Mbx%i WHERE hash NOT IN "
                        "(SELECT hash FROM Mbx%i)" % (otherindex, fileindex))

        u = 0
        while(True):      
            offset = cur.fetchone()
            if not offset: break
            f.write(msg(offset[0], filepaths[otherindex]))
            u += 1
        f.close()
        
        if u > 0:
            print ('%i unique messages found in: %s'
                   '\nThey have been written to: %s' %
                   (u, filepaths[otherindex], filepath))
            try:
                open(filepath + '.msf')
                print 'Removing ' + filepath + '.msf' 
                os.remove(filepath + '.msf')
            except IOError:
                '' # Do nothing


'''
Run
'''
try:
    path1 = sys.argv[1]
    path2 = sys.argv[2]
    files, uniquefiles1, uniquefiles2 = compare_paths(path1, path2)

    if files:
        for f in files:
            try:
                open('map.db')
                os.remove('map.db')
            except IOError:
                '' # Do nothing
        
            con = lite.connect('map.db')
            
            with con:
                cur = con.cursor()
                
                if os.path.basename(f[0]) == "popstate.dat":
                    insert_popstates(f, cur)
                    compare_popstates(f, cur)
                else:
                    store_hashes(f, cur) 
                    comparemsgs(f, cur)
                
                print "\n\n"
        print "Finished!"
        sys.exit(0)

except IndexError:
    print "Copyright (C) 2013 by Andre Gregori"
    print ("usage:  %s path_to_profile1 path_to_profile2" % 
           os.path.basename(sys.argv[0]))
    sys.exit(2)