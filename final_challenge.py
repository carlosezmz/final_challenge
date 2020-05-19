"""
    Big Data Final Challenge: Count the number of tickets per street segement from
                              2015-2019 in NYC.
                              
    @author: Carlos E. Tavarez Martinez
    
"""

# main
from pyspark import SparkContext
# from pyspark.sql import SQLContext




def check_name(st_name):
    
    """
        This function process street name if there is one and returns
        a street name with name and tyoe of street. 
        
        Example:
        
            'remsen street' --> 'REMSEN ST'
            'grand concourse avee' --> 'GRAND CONCOURSE AVE'
    """
    
    # check for some mis-spelling
    st_name = st_name.upper()
    
    st_name = st_name.replace('AVENUE', 'AVE')
    st_name = st_name.replace('AV', 'AVE')
    st_name = st_name.replace('AVE.', 'AVE')
    st_name = st_name.replace('AVEE', 'AVE')
    st_name = st_name.replace('STREET', 'ST')
    st_name = st_name.replace('ROAD', 'RD')
    st_name = st_name.replace('LN', 'LANE')
    st_name = st_name.replace('GD', 'GRAND')
    st_name = st_name.replace('BOULEVARD', 'BLVD')
    st_name = st_name.replace('BL', 'BLVD')
    st_name = st_name.replace('BLVDVD', 'BLVD')
    st_name = st_name.replace('DRIVE', 'DR')
    st_name = st_name.replace('PLACE', 'PL')
    st_name = st_name.replace('PARKWAY', 'PWY')
    st_name = st_name.replace('PKWY', 'PWY')
    st_name = st_name.replace('EAST', 'E')
    st_name = st_name.replace('WEST', 'W')
    st_name = st_name.replace('NORTH', 'N')
    st_name = st_name.replace('SOUTH', 'S')
    st_name = st_name.replace('EXPRESSWAY', 'EXPWY')
    st_name = st_name.replace('BRDWAY', 'BROADWAY')
        
    st_name = st_name.split(' ') # make list of words
    st_name = [name for name in st_name if len(name) > 0] # get rid of empty strings
    
    if len(st_name) > 3: # this part adjust the strings to the adequate size to be processed
        
        if st_name[3] in ['ST', 'RD', 'AVE', 'BLVD', 'DR', 'PL', 'PWY', 'EXPWY']:
            
            st_name = st_name[1:4]
            
        elif st_name[2] in ['ST', 'RD', 'AVE', 'BLVD', 'DR', 'PL', 'PWY', 'EXPWY']:
            
            st_name = st_name[:3]
            
        elif st_name[1] in ['ST', 'RD', 'AVE', 'BLVD', 'DR', 'PL', 'PWY', 'EXPWY']:
            
            st_name = st_name[:2]
        

            
    if len(st_name) == 3:
        try: # check if is a number
            if st_name[1][-2:] in ['TH', 'ST', 'RD', 'ND']:
                st_name[1] = int(st_name[1][:-2])
                st_name[1] = str(st_name[1])
                
                
            else:
                st_name[1] = int(st_name[1])
                st_name[1] = str(st_name[1])
                
        except ValueError: # not shown as number, ex: `fifth avenue`
            st_name[1] = st_name[1]
            
                        
                        
    elif len(st_name) == 2:
        
        if st_name[0] in ['ST', 'RD', 'AVE', 'BLVD', 'DR', 'PL', 'PWY', 'EXPWY']:
            
            if not st_name[1] in ['ST', 'RD', 'AVE', 'BLVD', 'DR', 'PL', 'PWY', 'EXPWY']:

                st_name[0], st_name[1] = st_name[1], st_name[0] # invert: `AVE U` --> `U AVE`
            
        else:
            try: # check for numbers
                num = int(st_name[0][:-2])
            
                if st_name[0][-2:] in ['TH', 'ST', 'RD', 'ND']:
                    st_name[0] = int(st_name[0][:-2])
                    st_name[0] = str(st_name[0])
                
                else:
                    st_name[0] = int(st_name[0])
                    st_name[0] = str(st_name[0])
            
            except ValueError: # got a string
                st_name[0] = st_name[0]
                        
                    
    else: # for empty strings
        
        if not st_name: return None
            
        
    st_name = ' '.join(st_name)
            
    return st_name





def check_house_number(number):
    
    """
        This function process house number if there is one and returns 
        a tuple of size 2.
        
        Example:
        
                '' --> (0, 0)
                '121' --> (121, 0)
                '110-15' --> (110, 15)
                '128-23C' --> (128, 23)
                '120-001' --> (120, 1)  
    """
    
    if len(number) == 0: # check for empty strings
        number = (0, 0)
    
    elif (len(number) > 0) and (type(number) != list):
        
        if '-' in number: # remove hifens
            number = number.split('-')
            
            if not number[1]: 
                number[1] = 0
                
            if not number[0]: 
                number[0] = 0
                
            number[0] = get_digits(number[0])
            number[1] = get_digits(number[1])
            
            try:
                number = (int(number[0]), int(number[1]))
                    
            except ValueError:
                        
                    number = (0, 0)
            
        else: # process no hifens
            number = get_digits(number)
            try:
                number = (int(number), 0)
            
            except ValueError:
                number = (0, 0)
                
                    
    elif type(number) == int:
        number = (int(number), 0)
                    
    elif type(number) == list: # some mis-spelling
        number[0] = get_digits(number[0])
        number = (number[0], 0)
                    
    return number

def check_county(county):
    
    """
        This function returns the name of the county if it matches
        the dictionary encoded below.
    """
    
    boro_dict = {
        'ny':'new york', 'ne': 'new york', 'ma':'new york', 'mn':'new york', '1': 'new york', 'mh':'new york',
        'bx':'bronx', 'br':'bronx', '2': 'bronx',
        'k': 'brooklyn', 'ki':'brooklyn', 'bk':'brooklyn', '3':'brooklyn',
        'q': 'queens', 'qn':'queens', 'qu':'queens', '4':'queens',
        'r': 'staten island', 's':'staten island', 'st':'staten island', 
            '5':'staten island', 'ri':'staten island', 'si':'staten island'
                }
    try: 
        if len(county) > 2:# if it has the whole name
            county = boro_dict[county[:2]]
            
        elif (len(county) <= 2) & (len(county) > 0):
            county = boro_dict[county]
        
    except KeyError: # does not match

        county = ''
        
    return county


    

def get_digits(number):
    
    """
        This function get all digits from a string.
        
        Ecample:
        
                '11C' --> 11
    """
    
    import re
    
    if number:
    
        digits = re.findall(r'\d+', str(number))
    
        digits = ''.join(digits)
    
        if len(digits) > 0: return digits 
    
        return 0
    return 0






def street_bounds(l_low, l_hig, r_low, r_hig):
    
    """
        This function takes maximum and minimum numbers of both sides of the 
        street and returns the minimum of both lows and the maximum of boths
        higs. 
    """

    
    if len(l_low) == 0: l_low = (0, 0)
        
    if len(l_hig) == 0: l_hig = (0, 0)
        
    if len(r_low) == 0: r_low = (0, 0)
        
    if len(r_hig) == 0: r_hig = (0, 0)
        
    l_low = check_house_number(str(l_low))
    l_hig = check_house_number(str(l_hig))
    r_low = check_house_number(str(r_low))
    r_hig = check_house_number(str(r_hig))
        
    l_low = max(l_low, r_low)
    l_hig = max(l_hig, r_hig)
    
    return (l_low, l_hig)




        

def extract_cols(partId, records):
    
    """
        This function process the data from NYC Parking Tickets. 
        It only returns the rows that belong to the year specified
        in the file. It also checks that every ticket must have a 
        valid summos number and that the issue dat was properly written.
    """

    if partId==0:
        next(records)
        
    import csv
    from datetime import datetime
    
    
    reader = csv.reader(records)
    
    for row in reader:
        
        if len(row) == 43:
            
            county = check_county(row[21].lower()) # process county name
            number = check_house_number(str(row[23])) # process house number
            summos = check_summos(row[0]) # process summos number
            
            # proper date
            if not len(row[4]) > 9: continue 
                
            date = str(datetime.strptime(row[4], '%m/%d/%Y'))
            year = int(datetime.strptime(row[4], '%m/%d/%Y').year)
            st_name = check_name(row[24].lower()) # process street name
            
            # year of the file
            if year != year_file.value[0]: continue 

                
            if county in ['staten island', 'new york', 'bronx', 'brooklyn', 'queens']: # filter county
                
                # proper house number
                if (type(number[0]) == int) & (type(number[1]) == int) & (type(number) == tuple) & (number != (0, 0)): 
                    
                    if summos: # proper summos number
                        
                        if len(date) == 19:
                    
                            yield ((county, st_name), (number, year))
                        
                        

    
def extract_bounds(partID, records):
    
    """
        This function process the records in NYC Centerline that
        have a proper county, street name and house number bounds.
    """
    
    if partID == 0:
        next(records)
    
    import csv
    
    reader = csv.reader(records)
    

    for row in reader:
            
        county = check_county(row[13]) # process county
        
        if county in ['staten island', 'new york', 'bronx', 'brooklyn', 'queens']:
            phy_id = int(row[0])
            st_name1 = check_name(row[28])
#             st_name2 = check_name(row[29])
            
#             if st_name1 != st_name2: continue
                
            (l_low, l_hig) = street_bounds(row[1], row[3], row[4], row[5])
            
            if (l_hig != l_low) & (type(l_low) == tuple) & (type(l_hig) == tuple):
                
                if st_name1:
        
                        yield ((county, st_name1), (phy_id, l_low, l_hig))

            

            
 
    
def reduce_csv(_, records):
    
    """
        This function counts the number of ticket per year for each physical ID.
        After counting all tickets per year, it gets the rate of tickets per
        physical ID.
    """
    
    # counting variables
    old_phy_id = None
    current_phy_id = None
    x = [0, 0, 0, 0, 0]
    old_x = [0, 0, 0, 0, 0]
    
    
    years_list = [2015, 2016, 2017, 2018, 2019]
    
    for values in records:
        
        phy_id = values[0]
        
        year = values[1]
        
        idx = years_list.index(year)
        
        if (phy_id != current_phy_id) & (current_phy_id == None): # first record
            
            current_phy_id = phy_id
            x[idx] += 1
            
        elif (phy_id == current_phy_id): # keep counting
            
            x[idx] += 1
            
        else: # get rate and return as csv format
            
            old_phy_id = current_phy_id
            current_phy_id = phy_id
            
            old_x = tuple(x)
            x = [0, 0, 0, 0, 0]
            x[idx] += 1
            
            rate = ((old_x[4]-old_x[3])+(old_x[3]-old_x[2])+(old_x[2]-old_x[1])+(old_x[1]-old_x[0]))/4
            
            yield ','.join((str(old_phy_id), str(old_x[0]), str(old_x[1]), str(old_x[2]), str(old_x[3]), str(old_x[4]), str(rate)))
            
            
            
            
            
def filter_id(partID, records):
    
    """
        This function returns the physical ID and the year if
        the house number is within the bounds of the physical ID
        segment.
    """
    
    for row in records:
        
        if (row[0][0] >= row[1][1]) & (row[0][0] <= row[1][2]):
            
            yield (row[1][0], row[0][1])
            
            
            
            
def check_summos(summos):
    
    """
        This function makes sure summos number is an integer.
    """
    
    try:
        summos = int(summos)
        
        return summos
        
    except ValueError:
        
        return None
            

        
            
if __name__ == '__main__':
    
    sc = SparkContext()
    

    center_dir = '/data/share/bdm/nyc_cscl.csv'
    fie2015_dir = '/data/share/bdm/nyc_parking_violation/2015.csv'
    fie2016_dir = '/data/share/bdm/nyc_parking_violation/2016.csv'
    fie2017_dir = '/data/share/bdm/nyc_parking_violation/2017.csv'
    fie2018_dir = '/data/share/bdm/nyc_parking_violation/2018.csv'
    fie2019_dir = '/data/share/bdm/nyc_parking_violation/2019.csv'
    
    files_list = [fie2015_dir, fie2016_dir, fie2017_dir, fie2018_dir, fie2019_dir]
    
    bounds = sc.textFile(center_dir).mapPartitionsWithIndex(extract_bounds).distinct()
    

    
    for idx, file in enumerate(files_list):
        
        # broadcast the year of the file
        year = int(file[-8:-4])
        year_file = sc.broadcast([year])
        
        
        # load and process parking tickets violations
        rdd = sc.textFile(file)\
                .mapPartitionsWithIndex(extract_cols).distinct()
        
        # inner join with centerline and getting the physical ID
        rdd = rdd.join(bounds).values()\
                 .mapPartitionsWithIndex(filter_id)
            
        if idx == 0:
            
            parking_violations_list = rdd
            
        else:
            
            parking_violations_list = parking_violations_list.union(rdd).cache()
            

    
    parking_violations = parking_violations_list.sortByKey().mapPartitionsWithIndex(reduce_csv)
#     count_tickts = parking_violations.mapPartitionsWithIndex(count_tickets).reduce(lambda x,y: x+y)
    
    parking_violations.saveAsTextFile('nyc_tickets_count')
#     count_tickts.saveAsTextFile('total_count')
