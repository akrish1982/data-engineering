import csv
with open('input.csv', 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        church = row[0]  # Get the church name
        names = row[1:]  # Get the list of names
        
        # Print the church name and each name
        for name in names[1:]:
            print(f'"{church}","{name}"')