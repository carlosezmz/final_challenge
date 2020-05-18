"""
    @author: Carlos E. Tavarez Martinez
"""

# main
from pyspark import SparkContext
# from pyspark.sql import SQLContext




def check_name(st_name):
    
    CHECK_BOL = True
    
    st_name = st_name.upper().replace('AVENUE', 'AVE')
    st_name = st_name.upper().replace('AV', 'AVE')
    st_name = st_name.upper().replace('AVE.', 'AVE')
    st_name = st_name.replace('STREET', 'ST')
    st_name = st_name.replace('ROAD', 'RD')
    st_name = st_name.replace('BOULEVARD', 'BLVD')
    st_name = st_name.replace('BL', 'BLVD')
    st_name = st_name.replace('DRIVE', 'DR')
    st_name = st_name.replace('PLACE', 'PL')
    st_name = st_name.replace('PARKWAY', 'PY')
    st_name = st_name.replace('EAST', 'E')
    st_name = st_name.replace('WEST', 'W')
    st_name = st_name.replace('NORTH', 'N')
    st_name = st_name.replace('SOUTH', 'S')
    st_name = st_name.replace('EXPRESSWAY', 'EXPWY')
    
    st_name = st_name.split(' ')
    
    while CHECK_BOL:
        
        if st_name[-1] in ['ST', 'RD', 'AVE', 'BLVD', 'DR', 'PL', 'PY', 'EXPWY', 'E', 'W', 'S', 'N']:
            
            if len(st_name) == 3:
                try:
                    if st_name[1][-2:] in ['TH', 'ST', 'RD', 'ND']:
                        st_name[1] = int(st_name[1][:-2])
                        st_name[1] = str(st_name[1])
                
                    else:
                        st_name[1] = int(st_name[1])
                        st_name[1] = str(st_name[1])
                
                except ValueError:
                        st_name[1] = st_name[1]
                        
                CHECK_BOL = False
                        
            elif len(st_name) == 2:
                try:
                    if st_name[0][-2:] in ['TH', 'ST', 'RD', 'ND']:
                        st_name[0] = int(st_name[0][:-2])
                        st_name[0] = str(st_name[0])
                
                    else:
                        st_name[0] = int(st_name[0])
                        st_name[0] = str(st_name[0])
            
                except ValueError:
                        st_name[0] = st_name[0]
                        
                CHECK_BOL = False
                    
        elif st_name[1] in ['ST', 'RD', 'AVE', 'BLVD', 'DR', 'PL', 'PY', 'EXPWY', 'E', 'W', 'S', 'N']:
            st_name = st_name[:2]
        
        elif st_name[2] in ['ST', 'RD', 'AVE', 'BLVD', 'DR', 'PL', 'PY', 'EXPWY', 'E', 'W', 'S', 'N']:
            st_name = st_name[:3]
        
    st_name = ' '.join(st_name)
        
    if st_name == 'BRDWAY':
        st_name = 'BROADWAY'
            
    return st_name

def check_house_number(number):
    
    if len(number) == 0:
        number = (0, 0)
    
    elif (len(number) > 0) and (type(number) != list):
        
        if '-' in number:
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

#                 try:
                        
                    number = (0, 0)

#                 except ValueError:
        
#                     number = (number[0], 0)
            
        else:
            number = get_digits(number)
            try:
                number = (int(number), 0)
            
            except ValueError:
                number = (number, 0)
                
                    
    elif type(number) == int:
        number = (int(number), 0)
                    
    elif type(number) == list:
        number[0] = get_digits(number[0])
        number = (number[0], 0)
                    
    return number

def check_county(county):
    
    boro_dict = {
        'ny':'new york', 'ne': 'new york', 'ma':'new york', 'mn':'new york', '1': 'new york', 'mh':'new york',
        'bx':'bronx', 'br':'bronx', '2': 'bronx',
        'k': 'brooklyn', 'ki':'brooklyn', 'bk':'brooklyn', '3':'brooklyn',
        'q': 'queens', 'qn':'queens', 'qu':'queens', '4':'queens',
        'r': 'staten island', 's':'staten island', 'st':'staten island', 
            '5':'staten island', 'ri':'staten island'
                }
    try:
        if len(county) > 2:
            county = boro_dict[county[:2]]
            
        elif (len(county) <= 2) & (len(county) > 0):
            county = boro_dict[county]
        
    except KeyError:

        county = ''
        
    return county

  

def get_digits(number):
    
    import re
    
    if number:
    
        digits = re.findall(r'\d+', str(number))
    
        digits = ''.join(digits)
    
        if len(digits) > 0: return digits
    
        return 0
    return 0

def street_bounds(l_low, l_hig, r_low, r_hig):

    
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

def check_summos(summos):
    
    try:
        summos = int(summos)
        
        return summos
        
    except ValueError:
        
        return None


        

def extract_cols(partId, records):
    

    if partId==0:
        next(records)
        
    import csv
    from datetime import datetime
    
    
    reader = csv.reader(records)
    
    for row in reader:
        
        if len(row) == 43:
            
            county = check_county(row[21].lower())
            number = check_house_number(str(row[23]))
            summos = check_summos(row[0])
            
            if not len(row[4]) > 9: continue
                
#             date = str(datetime.strptime(row[4], '%m/%d/%Y'))
            year = int(datetime.strptime(row[4], '%m/%d/%Y').year)
            st_name = check_name(row[24].lower())
            
            if year != year_file.value[0]: continue

            if county in ['staten island', 'new york', 'bronx', 'brooklyn', 'queens']:
                
                if (type(number[0]) == int) & (type(number[1]) == int) & (type(number) == tuple) & (number != (0, 0)):
                    
                    if summos:
                        
                        if len(date) == 19:
                    
                            yield ((county, st_name), (number, year, summos))
                    
 
    
    
def extract_bounds(partID, records):
    
    if partID == 0:
        next(records)
    
    import csv
    
    reader = csv.reader(records)
    

    for row in reader:
            
        county = check_county(row[13])
        
        if county in ['staten island', 'new york', 'bronx', 'brooklyn', 'queens']:
            phy_id = int(row[0])
            st_name1 = check_name(row[28])
#             st_name2 = check_name(row[29])
            (l_low, l_hig) = street_bounds(row[1], row[3], row[4], row[5])
            
#             if st_name1 != st_name2: continue
            
            if (l_hig != l_low) & (type(l_low) == tuple) & (type(l_hig) == tuple):
        
                    yield ((county, st_name1), (phy_id, l_low, l_hig))

            


def get_id(partID, records):
    
    for row in records:
        
        if row[0][0] != row[1][0]: continue
            
        if (row[0][0] >= row[1][1]) & (row[0][0] <= row[1][2]):
            
            year_d = {2015:0, 2016:0, 2017:0, 2018:0, 2019:0}
            
            if row[0][2] in year_d:
                
                year_d[row[0][2]] = 1
                
                year_t = (year_d[2015], year_d[2016], year_d[2017], year_d[2018], year_d[2019])
            
                yield (row[0][1], year_t)
    
 
    
def reduce_csv(_, records):
    
    old_phy_id = None
    current_phy_id = None
    x = [0, 0, 0, 0, 0]
    old_x = [0, 0, 0, 0, 0]
    
    
    years_list = [2015, 2016, 2017, 2018, 2019]
    
    for values in records:
        
        phy_id = values[0]
        
        year = values[1]
        
        if year not in years_list: continue
            
        idx = years_list.index(year)
        
        if (phy_id != current_phy_id) & (current_phy_id == None):
            
            current_phy_id = phy_id
            x[idx] += 1
            
        elif (phy_id == current_phy_id):
            
            x[idx] += 1
            
        else:
            
            old_phy_id = current_phy_id
            current_phy_id = phy_id
            
            old_x = tuple(x)
            x = [0, 0, 0, 0, 0]
            x[idx] += 1
            
            rate = ((old_x[4]-old_x[3])+(old_x[3]-old_x[2])+(old_x[2]-old_x[1])+(old_x[1]-old_x[0]))/4
            
            yield ','.join((str(old_phy_id), str(old_x[0]), str(old_x[1]), str(old_x[2]), str(old_x[3]), str(old_x[4]), str(rate)))
            
            
def filter_id(partID, records):
    
    for row in records:
        
        if (row[0][0] >= row[1][1]) & (row[0][0] <= row[1][2]):
            
            yield (row[1][0], row[0][1])
            

        
            
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
        
        year = int(file[-8:-4])
        
        year_file = sc.broadcast([year])
        
            
        rdd = sc.textFile(file)\
                .mapPartitionsWithIndex(extract_cols).distinct()


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
