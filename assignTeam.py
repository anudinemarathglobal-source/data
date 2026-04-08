import requests
import time
import re
import csv

# =========================
# CONFIG
# =========================

API_URL = "https://public.doubletick.io/customer/assign"

# 🔴 IMPORTANT: USE A NEW API KEY (old one exposed)
API_KEY = "key_RVrp5G2SgoK5f0yZvlwkMlHF4aEX7Lq1H3NowR5OFtDI5dEzO9HLc62cBCafeDounk7pZCyfJrJwKOHHlF1g8VWGDR16eDqP6cMgv9FAviv4aZ8Ih1EReOKWe14N4VcF8iVC69Q4Fvk5VDxsXMjTampZDPm1DOo45WvyP4UNfyZ3A2ZubjqtyWQ00F92hcTNSxIDs9Sqr7uhmjDQqM0qRqHy849Iq5v9JPWZTnNoqT2BteTOSButZ21AoiO6"

WABA_NUMBER = "+971521367907"
DELAY = 0.15

# =========================
# TEAM MEMBERS (VALID FORMAT)
# =========================

# TEAM_MEMBERS = [
#     # "+918075257358",
#     # "+918347101111",
#     # "+917356565921",
#     # "+917736348315",
#     # "+918848344688",
#     # "+918589064197",
# ]

TEAM_MEMBERS = [
    "+918921652881",  # SHAMNA
    "+918891890464",  # NEHA
    # "+918547573382",  # RAHIB
    "+918089898567",  # ZAKIYA
    # "+918714661951",  # ADWAITHA
    # "+918590966727",  # ARSHAD
    "+916238427287",  # SHIHAD 
    "+919778163168",  # AKASH
    "+918590170256",  # NAJIYA
    "+918593978664",  # RANJITH
    "+919495930870",  # AFNAN
]

# =========================
# CUSTOMERS
# =========================

CUSTOMERS = [
    "971569691765",
    "919943253156",
    "971543515761",
    "966564801737",
    "918176982817",
]

# =========================
# CLEAN FUNCTION
# =========================

def clean_numbers(numbers):
    cleaned = []
    for n in numbers:
        num = re.sub(r"\D", "", str(n))
        if 7 <= len(num) <= 15:
            cleaned.append(num)
    return list(dict.fromkeys(cleaned))


customers = clean_numbers(CUSTOMERS)
team = TEAM_MEMBERS

# =========================
# VALIDATION
# =========================

if not customers:
    print("❌ No customers provided")
    exit()

if not team:
    print("❌ No team members provided")
    exit()

print(f"\nCustomers: {len(customers)}")
print(f"Agents: {len(team)}")

# =========================
# HEADERS
# =========================

headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "Authorization": API_KEY
}

# =========================
# TEST API
# =========================

print("\n🔍 Testing API...")

test_payload = {
    "customerPhoneNumber": customers[0],
    "assignedUserPhoneNumber": team[0],
    "wabaNumber": WABA_NUMBER
}

test_res = requests.post(API_URL, json=test_payload, headers=headers)

print("Test:", test_res.status_code, test_res.text)

if test_res.status_code not in [200, 201]:
    print("❌ API ERROR — Check API key / permissions / number format")
    exit()

print("✅ API OK — Starting assignment...\n")

# =========================
# ROUND ROBIN ASSIGNMENT
# =========================

assignment_map = {}
success = 0
failed = 0

for i, customer in enumerate(customers):
    agent = team[i % len(team)]

    payload = {
        "customerPhoneNumber": customer,
        "assignedUserPhoneNumber": agent,
        "wabaNumber": WABA_NUMBER
    }

    try:
        res = requests.post(API_URL, json=payload, headers=headers)

        if res.status_code in [200, 201]:
            print(f"✅ {customer} → {agent}")
            assignment_map.setdefault(agent, []).append(customer)
            success += 1
        else:
            print(f"❌ {customer} → {agent} | {res.text}")
            failed += 1

    except Exception as e:
        print(f"⚠️ ERROR {customer} → {agent} | {e}")
        failed += 1

    time.sleep(DELAY)

# =========================
# FINAL OUTPUT
# =========================

print("\n===== FINAL DISTRIBUTION =====")

for agent, custs in assignment_map.items():
    print(f"\n👤 {agent} ({len(custs)} customers)")
    for c in custs:
        print(" -", c)

print("\n===== SUMMARY =====")
print("Total:", len(customers))
print("Success:", success)
print("Failed:", failed)

# =========================
# EXPORT CSV
# =========================

with open("assignment.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Agent", "Customer"])

    for agent, custs in assignment_map.items():
        for c in custs:
            writer.writerow([agent, c])

print("\n📁 assignment.csv saved")
