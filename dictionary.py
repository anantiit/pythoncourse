#Equivalent to map

monthConversions = {
    "Jan": "January",
    "Feb": "February",
    "Mar": "March"
}
print(monthConversions.get("Jan"))
print(monthConversions.get("Idiot"))
print(monthConversions.get("Idiot","Not a valid key"))

