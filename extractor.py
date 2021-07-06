import sqlite3
import csv
import sys
import os
import codecs
from datetime import datetime


def fmt_phone(number, name):
    if number is None:
        return
    number = number.replace(' ', '').replace('-', '').replace('+', '')
    if number == '':
        return
    if not number.startswith('549'):
        number = '549' + number
    contacts_dict[number] = name


contacts_file = sys.argv[1]
db_path = sys.argv[2]
dest_path = sys.argv[3]

dest_path_contacts = dest_path+'\\contacts'
os.mkdir(dest_path_contacts)
dest_path_groups = dest_path+'\\groups'
os.mkdir(dest_path_groups)

# TODO: replace unicode chars for files
# get contact list from csv
contacts_dict = {}
with open(contacts_file, newline='') as csv_file:
    csv_reader = csv.reader(csv_file)
    next(csv_reader, None)  # skip first item.
    for row in csv_reader:
        name = row[0]
        fmt_phone(row[34], name+" - 1")
        fmt_phone(row[36], name+" - 2")
        fmt_phone(row[38], name+" - 3")

# get groups from whatsapp db
groups_dict = {}
wa_db_path = db_path+"\\wa.db"
print("reading from "+wa_db_path)
with sqlite3.connect(wa_db_path) as con_wa:
    cur_wa = con_wa.cursor()
    groups = cur_wa.execute("""
        SELECT jid, display_name
        FROM wa_contacts
        WHERE display_name IS NOT NULL""")

for row in groups:
    id = row[0]
    name = row[1]
    groups_dict[id] = name

# get messages from whatsapp db
ms_db_path = db_path+"\\msgstore.db"
print("reading from "+ms_db_path)
with sqlite3.connect(ms_db_path) as con_ms:
    cur_ms1 = con_ms.cursor()
    chat_ids = cur_ms1.execute("""
        SELECT DISTINCT key_remote_jid
        FROM messages""")

    for chat_id in chat_ids:
        if chat_id[0].endswith("@s.whatsapp.net"):  # is a contact
            # get name
            phone = chat_id[0].split('@')[0]
            if phone in contacts_dict:
                name = contacts_dict[phone]
            else:
                name = phone

            # query messages
            cur_ms2 = con_ms.cursor()
            messages = cur_ms2.execute("""
                SELECT data, timestamp, media_mime_type, key_from_me, media_caption
                FROM messages
                WHERE key_remote_jid = '"""+chat_id[0]+"""'
                ORDER BY timestamp""")

            # create file
            file_name = dest_path_contacts+"\\"+name+".txt"
            print("writing at "+file_name)
            with codecs.open(file_name, "x", "utf-8-sig") as file:
                for ms in messages:
                    res_data = ms[0]
                    res_timestamp = ms[1]
                    res_media_mime_type = ms[2]
                    res_key_from_me = ms[3]
                    res_media_caption = ms[4]

                    content = ''
                    if res_data is not None:  # TODO: sent img
                        content = res_data
                    elif res_media_mime_type is not None:
                        content = '['+res_media_mime_type+']'
                        if res_media_caption is not None:
                            content = content + ' ' + res_media_caption

                    dt = datetime.utcfromtimestamp(int(res_timestamp)/1000)
                    dt_str = dt.strftime('%Y-%m-%d %H:%M:%S')

                    if res_key_from_me == 1:
                        separator = '>>>'
                    else:
                        separator = '<<<'

                    line = dt_str+' '+separator+' '+content+'\n'
                    file.write(line)

        # TODO: groups
