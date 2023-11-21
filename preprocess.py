import re

# Your unstructured input string
input_string = '''
[{Ultimate Parent, unity global holdings pte, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, null, null, null, null, null, [{Immediate Parent, mbh corporation plc, {business, /documentID[gb-experian-gbr100581906]/businessId[gbr162877966], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, null, null, null, null, [{Subject, GUILDPRIME SPECIALIST CONTRACTS LTD, {business, /documentID[gb-experian-gbr142770565], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[gbr162877966], experian}, 1 Guildprime Business Centre, Southend Road, BILLERICAY, Essex, CM11 2PZ, null, null, 4785440.0, []}]}, {Sibling, VICTORIA GOSDEN TRAVEL LTD, {business, /documentID[gb-edw-57863323]/businessId[gb-edw-57863323]/businessId[gb-edw-57863323], edw}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, 138, NORTH LANE, ALDERSHOT, Hampshire, GU12 4QN, United Kingdom, null, null, 1410200.0, []}, {Sibling, APPROVED AIR LIMITED, {business, /documentID[gb-edw-66137525]/businessId[gb-edw-66137525]/businessId[gb-edw-66137525], edw}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, Flat: THE CORN HOUSE, THE STABLES BUSINESS PARK, BRISTOL ROAD, ROOKSBRIDGE, AXBRIDGE, SOMERSET, BS26 2TT, UNITED KINGDOM, null, null, null, []}, {Sibling, ACADEMY 1 SPORTS LTD, {business, /documentID[gb-edw-72495954]/businessId[gb-edw-72495954]/businessId[gb-edw-72495954], edw}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, OLD GRANARY, DUNTON ROAD, BASILDON, SS15 4DB, UNITED KINGDOM, null, null, 1899560.0, []}, {Sibling, robinsons caravans holding company ltd, {business, /documentID[gb-experian-gbr100581906], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, 6-8 Ringwood Road, Brimington, CHESTERFIELD, Derbyshire, S43 1DG, null, 1005422.0, 4.1508548E7, []}, {Sibling, 3 K's Engineering Company Ltd, {business, /documentID[gb-experian-gbr100654862], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, Embankment Road, LLANELLI, United Kingdom, null, null, 2880150.0, []}, {Sibling, Gs Contracts (Joinery) Ltd, {business, /documentID[gb-experian-gbr103688323], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, G South House, COLCHESTER, United Kingdom, null, null, 4897000.0, []}, {Sibling, Du Boulay Contracts Ltd, {business, /documentID[gb-experian-gbr104916261], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, Royal Victoria Patriotic BuildingStudio 7, London, United Kingdom, null, -60518.0, 9151140.0, []}, {Sibling, ACACIA TRAINING LIMITED, {business, /documentID[gb-experian-gbr112379113], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, Acacia House  Trentham Business Quarter,  Bellringer Road, STOKE-ON-TRENT, United Kingdom, null, null, 1816800.0, []}, {Sibling, SHH Topco Ltd, {business, /documentID[gb-experian-gbr114772057], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, Samuel Hobson House, 20 Knutton Road, Wolstanton, Newcastle-Under-Lyme, NEWCASTLE, Staffordshire, ST5 0HU, null, null, 3560866.0, []}, {Sibling, Take Me (Stoke) Ltd, {business, /documentID[gb-experian-gbr124355101], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, 86 Old Town Road, STOKE-ON-TRENT, ST1 2JT, null, null, 1057000.0, []}, {Sibling, parenta group ltd, {business, /documentID[gb-experian-gbr129638623], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, 2-8 London Road, MAIDSTONE, Kent, ME16 8PZ, null, null, null, []}, {Sibling, Riverside Cabco Ltd, {business, /documentID[gb-experian-gbr141422549], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, 37 Florence Road, FLEET, Hampshire, GU52 6LG, null, null, null, []}, {Sibling, Gaysha Ltd, {business, /documentID[gb-experian-gbr143107847], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, Unit 1, Crown House, Queen Street, BEXLEYHEATH, Kent, DA7 4BT, null, null, null, []}, {Sibling, Parenta Training Ltd, {business, /documentID[gb-experian-gbr144539676], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, 2-8 London Road, MAIDSTONE, Kent, ME16 8PZ, null, null, null, []}, {Sibling, Logistica Training & Consultancy Ltd, {business, /documentID[gb-experian-gbr144930381], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, Enterprise Centre, Hedingham Grove, BIRMINGHAM, B37 7TP, null, null, null, []}, {Sibling, Logistica Training Ltd, {business, /documentID[gb-experian-gbr155709595], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, The Old Co-op Buildinbg, 11 Railway Street, GLOSSOP, Derbyshire, SK13 7AG, null, null, null, []}, {Sibling, VGT Taxis Ltd, {business, /documentID[gb-experian-gbr163653824], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, 15 Kings Road, FLEET, Hampshire, GU51 3AA, null, null, 252000.0, []}, {Sibling, UK Sports Training Ltd, {business, /documentID[gb-experian-gbr236362574], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, The Retreat, 406 Roding Lane South, WOODFORD GREEN, Essex, IG8 8EY, null, -7748.0, 426526.0, []}, {Sibling, 4X Ltd, {business, /documentID[gb-experian-gbr244029182], experian}, {business, /documentID[gb-experian-gbr100581906]/businessId[sgpf10000923], experian}, Steve's Garage Ermine Street, Little Stukeley, HUNTINGDON, Cambridgeshire, PE28 4BE, null, -916023.0, 1789786.0, []}]}]
'''

# Patterns to extract information
ultimate_parent_pattern = r'\{Ultimate Parent,\s([^,]+),\s(?:{business,([^}]+)experian},\s*)+([^,]+,\s[^,]+,\s[^,]+)'
immediate_parent_pattern = r'\{Immediate Parent,\s([^,]+),\s(?:{business,([^}]+)experian},\s*)+([^,]+,\s[^,]+,\s[^,]+)'
subject_pattern = r'\{Subject,\s([^,]+),\s(?:{business,([^}]+)experian},\s*)+([^,]+,\s[^,]+,\s[^,]+)'
sibling_pattern = r'\{Sibling,\s([^,]+),\s(?:{business,([^}]+)experian},\s*)+([^,]+,\s[^,]+,\s[^,]+)'

# Function to extract information using the given pattern
def extract_information(input_str, pattern,company_type):
    matches = re.finditer(pattern, input_str)
    extracted_data = []

    for match in matches:
        print("Match:", match.groups())  # Print the matched groups for debugging
        company_name, doc_id, address = match.groups()

        # Placeholder for additional information specific to each relation
        additional_info = None

        extracted_data.append({
            company_type: company_name,
            'document_id': doc_id,
            'address': address.replace(', null','').replace('null','')
        })

    return extracted_data

# Extract information for each relation using the corresponding pattern
ultimate_parent_data = extract_information(input_string, ultimate_parent_pattern,'Ultimate Parent')
immediate_parent_data = extract_information(input_string, immediate_parent_pattern,'Immediate Parent')
subject_data = extract_information(input_string, subject_pattern,'Subject')
sibling_data = extract_information(input_string, sibling_pattern,'Sibling')

# Print the extracted data for each relation
print("Ultimate Parent Data:", ultimate_parent_data)
print("Immediate Parent Data:", immediate_parent_data)
print("Subject Data:", subject_data)
print("Sibling Data:", sibling_data)
