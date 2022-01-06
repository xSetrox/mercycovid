import requests 
from bs4 import BeautifulSoup
from time import sleep
import sqlite3

# set this 
webhook_url = ""
print("Connecting to DB...")
con = sqlite3.connect('data.db')
cur = con.cursor()
print("DB connected, creating table if it does not exist...")
cur.execute("CREATE TABLE IF NOT EXISTS spring2022 (campus text, active integer, recovered integer)")
cur.execute("CREATE TABLE IF NOT EXISTS vaccines (key text, val integer)")
cur.execute("CREATE TABLE IF NOT EXISTS tests (key text, val integer)")
con.commit()
print("Done with initializing")

def get_active_recovered(soup):
    table = soup.find('table').tbody
    # each row
    children = table.findChildren('tr')[1:]
    data = []
    for c in children:
        # each column
        cells = c.findChildren('td')
        vals = {
            "campus": cells[0].text
        }
        try:
            vals['active'] = int(cells[1].text)
        except:
            vals['active'] = 0
        
        try:
            vals['recovered'] = int(cells[2].text)
        except:
            vals['recovered'] = 0

        data.append(vals)
    return data

def get_test_stats(soup):
    finds = soup.find_all('p')
    for f in finds:
        if "On-Campus Tests Administered" in f.text:
            find = f.text
            break
    find = find.split('\n')
    administered = int(find[0].split()[-1:][0].replace(',', ''))
    positives = int(find[1].split()[-1:][0].replace(',', ''))
    positivity = int(float(find[2].split()[-1:][0].replace('%', '').replace(',', ''))*10)
    data = {
        'administered': administered,
        'positives' : positives,
        'positivity': positivity
    }
    return data

def get_vaccine_data(soup):
    find = soup.find('td', text='Percentage Fully Vaccinated').parent.parent
    children = find.findChildren('tr')[1:]
    data_pre = []
    for c in children:
        # each column
        cells = c.findChildren('td')
        data_pre.append([cells[0].text, int(cells[1].text.replace('%', ''))])
    data = {}
    data['students'] = data_pre[0][1]
    data['staff'] = data_pre[1][1]
    data['fulltime'] = data_pre[2][1]
    data['parttime'] = data_pre[2][1]
    return data

while True:
    changed = False
    change_text = "The following changes were detected on the Mercy Covid Dashboard: \n"
    page = requests.get("https://www.mercy.edu/campus-life/fall-2021-return-campus/covid-19-dashboard").text
    soup = BeautifulSoup(page, "lxml")
    # cases
    active_recovered = get_active_recovered(soup)
    for d in active_recovered:
        find = cur.execute("SELECT * FROM spring2022 WHERE campus = ?", (d['campus'],)).fetchall()
        if not find:
            print(f"Inserting values into database: {d['campus']}, {d['active']}, {d['recovered']}")
            cur.execute("INSERT INTO spring2022 VALUES (?, ?, ?)", (d['campus'], d['active'], d['recovered']))
            con.commit()
            changed = True
            change_text += f"• Data for the {d['campus']} campus has been added: {d['active']} active cases, {d['recovered']} recovered cases\n"
        else:
            if list(d.values()) != list(find[0]):
                changed = True
                new = list(d.values())
                db = list(find[0])
                if new[1] != db[1]:
                    change_text += f"• Active cases for {new[0]} campus changed from {db[1]} to {new[1]}\n"
                    cur.execute("UPDATE spring2022 SET active = ? WHERE campus = ?", (new[1], new[0]))
                if new[2] != db[2]:
                    change_text += f"• Recovered cases for {new[0]} campus changed from {db[2]} to {new[2]}\n"
                    cur.execute("UPDATE spring2022 SET recovered = ? WHERE campus = ?", (new[2], new[0]))
                con.commit()
    # tests
    test_data = get_test_stats(soup)
    find = cur.execute("SELECT * FROM tests").fetchall()
    if not find:
        print(f"Inserting values into database: {test_data['administered']}, {test_data['positives']}, {test_data['positivity']}")
        cur.execute("INSERT INTO tests VALUES (?, ?)", ("Administered", test_data['administered']))
        cur.execute("INSERT INTO tests VALUES (?, ?)", ("Positives", test_data['positives']))
        cur.execute("INSERT INTO tests VALUES (?, ?)", ("Positivity", test_data['positivity']))
        con.commit()
        changed = True
        change_text += f"• Data for tests has been added.\n"
    else:
        for f in find:
            lower = f[0].lower()
            # f[1] is old, test_data[lower] is new
            if f[1] != test_data[lower]:
                changed = True
                change_text += f"• Test data changed: {lower.title()} changed from {f[1]} to {test_data[lower]}"
                if lower == "positivity":
                    change_text += '%'
                change_text += '\n'
                cur.execute("UPDATE tests SET val = ? WHERE key = ?", (test_data[lower], lower))
                con.commit()
    # vaccines
    find = cur.execute("SELECT * FROM vaccines").fetchall()
    vaccine_data = get_vaccine_data(soup)
    if not find:
        print(f"Inserting values into database: {vaccine_data['students']}, {vaccine_data['staff']}, {vaccine_data['fulltime']}, {vaccine_data['parttime']}")
        cur.execute("INSERT INTO vaccines VALUES (?, ?)", ("Students", vaccine_data['students']))
        cur.execute("INSERT INTO vaccines VALUES (?, ?)", ("Staff", vaccine_data['staff']))
        cur.execute("INSERT INTO vaccines VALUES (?, ?)", ("Fulltime", vaccine_data['fulltime']))
        cur.execute("INSERT INTO vaccines VALUES (?, ?)", ("Parttime", vaccine_data['parttime']))
        con.commit()
        changed = True
        change_text += f"• Data for vaccines has been added.\n"
    else:
        for f in find:
            title = f[0].lower()
            if vaccine_data[title] != f:
                change_text += f"• Vaccine data changed: {title} changed from {f[1]} to {vaccine_data[title]}%"
                cur.execute("UPDATE vaccines SET val = ? WHERE key = ?", (f[1], vaccine_data[title]))
                con.commit()

    if changed:
        data = {
            "username" : "Mercy Covid Alert",
            "content" : change_text + '\n\n(service developed by Lance)'
        }
        requests.post(url = webhook_url, json=data)
        
    print("Sleeping 30 seconds")
    sleep(30)