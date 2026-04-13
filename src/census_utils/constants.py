"""Shared constants for US Census Bureau connector."""

from datetime import datetime

CURRENT_YEAR = datetime.now().year

# Priority dataset patterns we want to collect
PRIORITY_DATASETS = {
    'acs': {'name': 'American Community Survey', 'min_year': 2010, 'max_year': CURRENT_YEAR - 1, 'priority': 1},
    'cbp': {'name': 'County Business Patterns', 'min_year': 2010, 'priority': 1},
    'pep': {'name': 'Population Estimates Program', 'min_year': 2010, 'priority': 1},
    'zbp': {'name': 'ZIP Business Patterns', 'min_year': 2010, 'priority': 2},
    'ase': {'name': 'Annual Survey of Entrepreneurs', 'min_year': 2014, 'priority': 2},
    'abscs': {'name': 'Annual Business Survey', 'min_year': 2017, 'priority': 2},
    'bdstimeseries': {'name': 'Business Dynamics Statistics', 'min_year': 2010, 'priority': 2},
    'intltrade': {'name': 'International Trade', 'min_year': 2013, 'priority': 1},
    'sahie': {'name': 'Small Area Health Insurance Estimates', 'min_year': 2010, 'priority': 2},
    'saipe': {'name': 'Small Area Income and Poverty Estimates', 'min_year': 2010, 'priority': 1},
    'ecnbasic': {'name': 'Economic Census', 'min_year': 2017, 'priority': 1},
    'bps': {'name': 'Building Permits Survey', 'min_year': 2010, 'priority': 1}
}

# Priority variable patterns by dataset type
PRIORITY_VARIABLES = {
    'acs': {
        'B01001': 'Sex by Age', 'B02001': 'Race', 'B03001': 'Hispanic Origin',
        'B19013': 'Median Household Income', 'B19001': 'Household Income',
        'B17001': 'Poverty Status', 'B22001': 'SNAP', 'B25001': 'Housing Units',
        'B25077': 'Median Home Value', 'B25003': 'Tenure', 'B25061': 'Rent',
        'B23025': 'Employment Status', 'B24010': 'Occupation',
        'B08301': 'Commute Mode', 'B08303': 'Commute Time',
        'B15003': 'Educational Attainment', 'B14001': 'School Enrollment'
    },
    'cbp': {'EMP': 'Employees', 'PAYANN': 'Annual Payroll', 'ESTAB': 'Establishments', 'EMPSZES': 'Employment Size', 'PAYQTR1': 'Q1 Payroll'},
    'pep': {'POP': 'Population', 'DENSITY': 'Density', 'BIRTHS': 'Births', 'DEATHS': 'Deaths', 'NATURALINC': 'Natural Increase'},
    'saipe': {'SAEPOVRTALL_PT': 'Poverty Rate', 'SAEPOVRT0_17_PT': 'Child Poverty Rate', 'SAEMHI_PT': 'Median Income'},
    'sahie': {'NIC_PT': 'Insured Count', 'NIPR_PT': 'Insured Rate', 'NUI_PT': 'Uninsured Count'},
    'intltrade': {'IMPVAL': 'Import Value', 'EXPVAL': 'Export Value', 'BALANCE': 'Trade Balance'},
}

GEOGRAPHY_LEVELS = {
    'state': 'state:*',
    'county': 'county:*',
}

MAX_VARS_PER_REQUEST = 49  # Census API allows 50 total, but NAME is always included
