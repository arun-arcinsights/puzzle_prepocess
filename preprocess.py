import re

def extract_patterns(input_string):
    ultimate_parent_pattern = r'{Ultimate Parent, (.*?),'
    immediate_parent_pattern = r'{Immediate Parent, (.*?),'
    subject_pattern = r'{Subject, (.*?),'
    sibling_pattern = r'{Sibling, (.*?),'

    document_pattern = r'{business, (.*?), experian}'
    address_pattern = r'null, (.+?), (.+?), (.+?), (.+?), (.+?), (.+?), (.+?), (.+?), (.+?), (.+?), (.+?),'

    ultimate_parent_match = re.search(ultimate_parent_pattern, input_string)
    immediate_parent_match = re.search(immediate_parent_pattern, input_string)
    subject_match = re.search(subject_pattern, input_string)

    sibling_matches = re.finditer(sibling_pattern, input_string)

    ultimate_parent_info = None
    immediate_parent_info = None
    subject_info = None
    sibling_info = []

    if ultimate_parent_match:
        ultimate_parent_info = {
            'Company Name': ultimate_parent_match.group(1),
            'Documents': re.search(document_pattern, input_string).group(1),
            'Addresses': re.search(address_pattern, input_string).group(1, 2, 3, 4, 5, 6)  # Adjust index accordingly
        }

    if immediate_parent_match:
        immediate_parent_info = {
            'Company Name': immediate_parent_match.group(1),
            'Documents': re.search(document_pattern, input_string).group(1),
            'Addresses': re.search(address_pattern, input_string).group(1, 2, 3, 4, 5, 6)  # Adjust index accordingly
        }

    if subject_match:
        subject_info = {
            'Company Name': subject_match.group(1),
            'Documents': re.search(document_pattern, input_string).group(1),
            'Addresses': re.search(address_pattern, input_string).group(1, 2, 3, 4, 5, 6)  # Adjust index accordingly
        }

    for sibling_match in sibling_matches:
        sibling_input = input_string[sibling_match.end():]
        document_match = re.search(document_pattern, sibling_input)
        address_match = re.search(address_pattern, sibling_input)
        if document_match and address_match:
            sibling = {
                'Company Name': sibling_match.group(1),
                'Documents': document_match.group(1),
                'Addresses': address_match.group(1, 2, 3, 4, 5, 6)  # Adjust index accordingly
            }
            sibling_info.append(sibling)

    return ultimate_parent_info, immediate_parent_info, subject_info, sibling_info

# Your input string
input_string = '''
[{Ultimate Parent, mears group plc, {business, /documentID[gb-experian-gbr100211359]/businessId[gbr108593510], experian}, null, 1390 Montpellier Court, Gloucester Business Park, Brockworth, GLOUCESTER, GL3 4AH, null, 5824000.0, 1.599355E8, [{Immediate Parent, target healthcare ltd, {business, /documentID[gb-experian-gbr142782555]/businessId[gbr154598959], experian}, {business, /documentID[gb-experian-gbr100211359]/businessId[gbr108593510], experian}, 8 Redwood Crescent, East Kilbride, GLASGOW, G74 5PA, null, 794915.0, 5.1981152E7, [{Subject, Quantum Pharmaceutical Ltd, {business, /documentID[gb-experian-gbr142782555], experian}, {business, /documentID[gb-experian-gbr142782555]/businessId[gbr154598959], experian}, Unit 11, Hobson Industrial Estate, Hobson, NEWCASTLE UPON TYNE, NE16 6EA, null, 6948146.0, 1.18967432E8, []}]}, {Sibling, Neesom Investments Ltd, {business, /documentID[gb-experian-gbr100211359], experian}, {business, /documentID[gb-experian-gbr100211359]/businessId[gbr108593510], experian}, Hunmanby Industrial Estate, Hunmanby, FILEY, North Yorkshire, YO14 0PH, null, 368568.0, 4573948.0, []}, {Sibling, Scion Property Services Ltd, {business, /documentID[gb-experian-gbr100325283], experian}, {business, /documentID[gb-experian-gbr100325283]/businessId[gbr102543959], experian}, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, []}]}]
'''

# Extract patterns
ultimate_parent_info, immediate_parent_info, subject_info, sibling_info = extract_patterns(input_string)

# Print the results
print('Ultimate Parent Info:', ultimate_parent_info)
print('\nImmediate Parent Info:', immediate_parent_info)
print('\nSubject Info:', subject_info)
print('\nSibling Info:', sibling_info)