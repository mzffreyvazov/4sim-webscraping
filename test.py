import requests

# --- Paste the EXACT SAME proxy URL you are using in the main script ---
PROXY_URL = "http://spi8e1zees:HX83+0fdkrhNouutR4@az.decodo.com:30001" # <--- EDIT THIS LINE

PROXIES = {
   "http": PROXY_URL,
   "https": PROXY_URL,
} if "YOUR_FULL_PASSWORD" not in PROXY_URL else {}

# We will try to connect to a simple test site that just shows our IP address.
test_url = "https://httpbin.org/ip"

print(f"Testing proxy connection with: {PROXY_URL}")

if not PROXIES:
    print("!!! ERROR: PROXY_URL is not configured. Please edit the script. !!!")
else:
    try:
        response = requests.get(test_url, proxies=PROXIES, timeout=30)
        response.raise_for_status() # Raise an error if the status is not 200 OK

        # If successful, this will print the IP address of your PROXY
        print("\nSUCCESS! Proxy connection is working.")
        print("Your request is coming from this IP address:")
        print(response.json())

    except Exception as e:
        # If it fails, it will print the error
        print("\n---!!! FAILURE !!!---")
        print("The proxy connection failed. This is not a problem with the scraper, but with the proxy setup.")
        print(f"Error: {e}")
        print("\nTroubleshooting steps:")
        print("1. Did you set the proxy Location/Country to Azerbaijan in your dashboard?")
        print("2. Did you copy the USERNAME, PASSWORD, HOST, and PORT correctly?")
        print("3. Did you add your current IP address to the 'Whitelisted IPs' in your dashboard?")