from nifiapi.flowfilesource import FlowFileSource, FlowFileSourceResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

import random
from datetime import date, timedelta
import json
import pprint

class CreateFamilyGraphFlowFile(FlowFileSource):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileSource']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = 'Generates a FlowFile with insurance info.'
        tags = ["Insurance", "graph", "gremlin", "janus"]

    def __init__(self, **kwargs):
        pass
#        super().__init__(**kwargs)

    def getPropertyDescriptors(self):
        # Define any properties for your processor here if needed
        return []

    def create(self, context):
        # Generate the content for the FlowFile
        # You can change this number to generate a smaller or larger dataset
        NUMBER_OF_FAMILIES = 1

        # --- Data for generating random people ---

        # 1. Generate the data structure
        generated_data = self.generate_all_data(NUMBER_OF_FAMILIES)

        content =  json.dumps(generated_data, indent=2)

        # Define attributes for the FlowFile
        attributes = {"fam_label": generated_data['summary']['fam_label'],"v1": "families", "v2": "members", "v3": "policies", "v4": "claims","e1": "relationships", "summary": "summary"}

        # Create and return the FlowFileSourceResult
        return FlowFileSourceResult(
            relationship="success",
            contents=content.encode('utf-8'),  # Content must be bytes
            attributes=attributes
        )


    def generate_all_data(self,num_families):
        """
        Generates random data for families, relationships, policies, and claims,
        and returns it as a dictionary of lists.
        """

        LAST_NAMES = [
            'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
            'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
            'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
            'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker',
            'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores',
            'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell',
            'Carter', 'Roberts', 'Gomez', 'Phillips', 'Evans', 'Turner', 'Diaz', 'Parker',
            'Cruz', 'Edwards', 'Collins', 'Reyes', 'Stewart', 'Morris', 'Morales', 'Murphy',
            'Cook', 'Rogers', 'Gutierrez', 'Ortiz', 'Morgan', 'Cooper', 'Peterson', 'Bailey',
            'Reed', 'Kelly', 'Howard', 'Ramos', 'Kim', 'Cox', 'Ward', 'Richardson', 'Watson',
            'Brooks', 'Chavez', 'Wood', 'James', 'Bennett', 'Gray', 'Mendoza', 'Ruiz',
            'Hughes', 'Price', 'Alvarez', 'Castillo', 'Sanders', 'Patel', 'Myers', 'Long',
            'Ross', 'Foster', 'Jimenez'
        ]
        MALE_FIRST_NAMES = [
            'James', 'Robert', 'John', 'Michael', 'David', 'William', 'Richard', 'Joseph',
            'Thomas', 'Charles', 'Christopher', 'Daniel', 'Matthew', 'Anthony', 'Mark',
            'Donald', 'Steven', 'Paul', 'Andrew', 'Joshua', 'Kevin', 'Brian', 'George',
            'Timothy', 'Ronald'
        ]
        FEMALE_FIRST_NAMES = [
            'Mary', 'Patricia', 'Jennifer', 'Linda', 'Elizabeth', 'Barbara', 'Susan',
            'Jessica', 'Sarah', 'Karen', 'Lisa', 'Nancy', 'Betty', 'Margaret', 'Sandra',
            'Ashley', 'Kimberly', 'Emily', 'Donna', 'Michelle', 'Carol', 'Amanda',
            'Melissa', 'Deborah', 'Stephanie'
        ]
        SEXES = ['M', 'F']

        # --- Data for Insurance Policies and Claims ---
        POLICY_TYPES = ['dental', 'life', 'disability', 'car']
        START_DATE = date(2023, 1, 1)
        END_DATE = date(2025, 10, 12)
        DATE_RANGE_DAYS = (END_DATE - START_DATE).days

        family_list = []
        members_list = []
        policies_list = []
        claims_list = []
        relationships_list = []

        claim_id_counter = 1
        summary_fam_label = None # Will hold the fam_label of the first family
        for _ in range(num_families):
            lastname = random.choice(LAST_NAMES)
            fam_id = f"{random.randint(0, 999999):06}"
            fam_label = f"{lastname}_{fam_id}"

            if summary_fam_label is None:
                summary_fam_label = fam_label

            family_list.append({
                'fam_id': fam_id,
                'lastname': lastname,
                'fam_label': fam_label
            })

            current_adults = []
            current_children = []
            used_child_firstnames = set()

            # -- Generate Members and "member_of" Relationships --
            num_parents = random.randint(1, 2)
            for _ in range(num_parents):
                sex = random.choice(SEXES)
                firstname = random.choice(MALE_FIRST_NAMES if sex == 'M' else FEMALE_FIRST_NAMES)
                age = random.randint(22, 55)
                current_adults.append({'firstname': firstname, 'age': age})
                members_list.append({
                    'fam_id': fam_id, 'lastname': lastname, 'firstname': firstname,
                    'adult_Y_N': 'Y', 'sex': sex, 'age': age,
                    'fam_label': fam_label
                })
                relationships_list.append({
                    'fam_id': fam_id,
                    'lastname': lastname,
                    'firstname': firstname,
                    'relationship_name': 'member_of',
                    'vertex1': fam_label,
                    'v1type': 'family',
                    'vertex2': f'{firstname}_{lastname}_{age}_{fam_id}',
                    'v2type': 'member',
                    'fam_label': fam_label
                })

            # -- Generate "has_spouse" Relationship Record --
            if len(current_adults) > 1:
                adult1 = current_adults[0]
                adult2 = current_adults[1]
                relationships_list.append({
                    'fam_id': fam_id,
                    'lastname': lastname,
                    'adult1_firstname': adult1['firstname'],
                    'adult2_firstname': adult2['firstname'],
                    'relationship_name': 'has_spouse',
                    'vertex1': f"{adult1['firstname']}_{lastname}_{adult1['age']}_{fam_id}",
                    'v1type': 'member',
                    'vertex2': f"{adult2['firstname']}_{lastname}_{adult2['age']}_{fam_id}",
                    'v2type': 'member',
                    'fam_label': fam_label
                })

            num_children = random.randint(0, 5)
            for _ in range(num_children):
                sex = random.choice(SEXES)
                name_list = MALE_FIRST_NAMES if sex == 'M' else FEMALE_FIRST_NAMES

                while True:
                    firstname = random.choice(name_list)
                    if firstname not in used_child_firstnames:
                        used_child_firstnames.add(firstname)
                        break

                age = random.randint(1, 22)
                current_children.append({'firstname': firstname, 'age': age})
                members_list.append({
                    'fam_id': fam_id, 'lastname': lastname, 'firstname': firstname,
                    'adult_Y_N': 'N', 'sex': sex, 'age': age,
                    'fam_label': fam_label
                })
                relationships_list.append({
                    'fam_id': fam_id,
                    'lastname': lastname,
                    'firstname': firstname,
                    'relationship_name': 'member_of',
                    'vertex1': fam_label,
                    'v1type': 'family',
                    'vertex2': f'{firstname}_{lastname}_{age}_{fam_id}',
                    'v2type': 'member',
                    'fam_label': fam_label
                })

            # -- Generate "has_child" Relationship Records --
            for adult in current_adults:
                for child in current_children:
                    relationships_list.append({
                        'fam_id': fam_id,
                        'lastname': lastname,
                        'adult_firstname': adult['firstname'],
                        'child_firstname': child['firstname'],
                        'relationship_name': 'has_child',
                        'vertex1': f"{adult['firstname']}_{lastname}_{adult['age']}_{fam_id}",
                        'v1type': 'member',
                        'vertex2': f"{child['firstname']}_{lastname}_{child['age']}_{fam_id}",
                        'v2type': 'member',
                        'fam_label': fam_label
                    })

            # -- Generate Policies and "has_policy" Relationships --
            for policy_type in POLICY_TYPES:
                if random.choice([True, False]):
                    policy_name = f"{fam_id}_{policy_type}"
                    policies_list.append({
                        'fam_id': fam_id, 'policy_name': policy_name,
                        'lastname': lastname, 'type': policy_type,
                        'fam_label': fam_label
                    })

                    relationships_list.append({
                        'fam_id': fam_id,
                        'policy_name': policy_name,
                        'relationship_name': 'has_policy',
                        'vertex1': fam_label,
                        'v1type': 'family',
                        'vertex2': policy_name,
        pass
#        super().__init__(**kwargs)

    def getPropertyDescriptors(self):
        # Define any properties for your processor here if needed
        return []

    def create(self, context):
        # Generate the content for the FlowFile
        # You can change this number to generate a smaller or larger dataset
        NUMBER_OF_FAMILIES = 1

        # --- Data for generating random people ---

        # 1. Generate the data structure
        generated_data = self.generate_all_data(NUMBER_OF_FAMILIES)

        content =  json.dumps(generated_data, indent=2)

        # Define attributes for the FlowFile
        attributes = {"fam_label": generated_data['summary']['fam_label'],"v1": "families", "v2": "members", "v3": "policies", "v4": "claims","e1": "relationships", "summary": "summary"}

        # Create and return the FlowFileSourceResult
        return FlowFileSourceResult(
            relationship="success",
            contents=content.encode('utf-8'),  # Content must be bytes
            attributes=attributes
        )


    def generate_all_data(self,num_families):
        """
        Generates random data for families, relationships, policies, and claims,
        and returns it as a dictionary of lists.
        """

        LAST_NAMES = [
            'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
            'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
            'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
            'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker',
            'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores',
            'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell',
            'Carter', 'Roberts', 'Gomez', 'Phillips', 'Evans', 'Turner', 'Diaz', 'Parker',
            'Cruz', 'Edwards', 'Collins', 'Reyes', 'Stewart', 'Morris', 'Morales', 'Murphy',
            'Cook', 'Rogers', 'Gutierrez', 'Ortiz', 'Morgan', 'Cooper', 'Peterson', 'Bailey',
            'Reed', 'Kelly', 'Howard', 'Ramos', 'Kim', 'Cox', 'Ward', 'Richardson', 'Watson',
            'Brooks', 'Chavez', 'Wood', 'James', 'Bennett', 'Gray', 'Mendoza', 'Ruiz',
            'Hughes', 'Price', 'Alvarez', 'Castillo', 'Sanders', 'Patel', 'Myers', 'Long',
            'Ross', 'Foster', 'Jimenez'
        ]
        MALE_FIRST_NAMES = [
            'James', 'Robert', 'John', 'Michael', 'David', 'William', 'Richard', 'Joseph',
            'Thomas', 'Charles', 'Christopher', 'Daniel', 'Matthew', 'Anthony', 'Mark',
            'Donald', 'Steven', 'Paul', 'Andrew', 'Joshua', 'Kevin', 'Brian', 'George',
            'Timothy', 'Ronald'
        ]
        FEMALE_FIRST_NAMES = [
            'Mary', 'Patricia', 'Jennifer', 'Linda', 'Elizabeth', 'Barbara', 'Susan',
            'Jessica', 'Sarah', 'Karen', 'Lisa', 'Nancy', 'Betty', 'Margaret', 'Sandra',
            'Ashley', 'Kimberly', 'Emily', 'Donna', 'Michelle', 'Carol', 'Amanda',
            'Melissa', 'Deborah', 'Stephanie'
        ]
        SEXES = ['M', 'F']

        # --- Data for Insurance Policies and Claims ---
        POLICY_TYPES = ['dental', 'life', 'disability', 'car']
[jprosser@go01-cod-edge-leader0 ~]$ cat  CreateFamilyGraphFlowFile.py
from nifiapi.flowfilesource import FlowFileSource, FlowFileSourceResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

import random
from datetime import date, timedelta
import json
import pprint

class CreateFamilyGraphFlowFile(FlowFileSource):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileSource']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = 'Generates a FlowFile with insurance info.'
        tags = ["Insurance", "graph", "gremlin", "janus"]

    def __init__(self, **kwargs):
        pass
#        super().__init__(**kwargs)

    def getPropertyDescriptors(self):
        # Define any properties for your processor here if needed
        return []

    def create(self, context):
        # Generate the content for the FlowFile
        # You can change this number to generate a smaller or larger dataset
        NUMBER_OF_FAMILIES = 1

        # --- Data for generating random people ---

        # 1. Generate the data structure
        generated_data = self.generate_all_data(NUMBER_OF_FAMILIES)

        content =  json.dumps(generated_data, indent=2)

        # Define attributes for the FlowFile
        attributes = {"fam_label": generated_data['summary']['fam_label'],"v1": "families", "v2": "members", "v3": "policies", "v4": "claims","e1": "relationships", "summary": "summary"}

        # Create and return the FlowFileSourceResult
        return FlowFileSourceResult(
            relationship="success",
            contents=content.encode('utf-8'),  # Content must be bytes
            attributes=attributes
        )


    def generate_all_data(self,num_families):
        """
        Generates random data for families, relationships, policies, and claims,
        and returns it as a dictionary of lists.
        """

        LAST_NAMES = [
            'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
            'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
            'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
            'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker',
            'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores',
            'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell',
            'Carter', 'Roberts', 'Gomez', 'Phillips', 'Evans', 'Turner', 'Diaz', 'Parker',
            'Cruz', 'Edwards', 'Collins', 'Reyes', 'Stewart', 'Morris', 'Morales', 'Murphy',
            'Cook', 'Rogers', 'Gutierrez', 'Ortiz', 'Morgan', 'Cooper', 'Peterson', 'Bailey',
            'Reed', 'Kelly', 'Howard', 'Ramos', 'Kim', 'Cox', 'Ward', 'Richardson', 'Watson',
            'Brooks', 'Chavez', 'Wood', 'James', 'Bennett', 'Gray', 'Mendoza', 'Ruiz',
            'Hughes', 'Price', 'Alvarez', 'Castillo', 'Sanders', 'Patel', 'Myers', 'Long',
            'Ross', 'Foster', 'Jimenez'
        ]
        MALE_FIRST_NAMES = [
            'James', 'Robert', 'John', 'Michael', 'David', 'William', 'Richard', 'Joseph',
            'Thomas', 'Charles', 'Christopher', 'Daniel', 'Matthew', 'Anthony', 'Mark',
            'Donald', 'Steven', 'Paul', 'Andrew', 'Joshua', 'Kevin', 'Brian', 'George',
            'Timothy', 'Ronald'
        ]
        FEMALE_FIRST_NAMES = [
            'Mary', 'Patricia', 'Jennifer', 'Linda', 'Elizabeth', 'Barbara', 'Susan',
            'Jessica', 'Sarah', 'Karen', 'Lisa', 'Nancy', 'Betty', 'Margaret', 'Sandra',
            'Ashley', 'Kimberly', 'Emily', 'Donna', 'Michelle', 'Carol', 'Amanda',
            'Melissa', 'Deborah', 'Stephanie'
        ]
        SEXES = ['M', 'F']

        # --- Data for Insurance Policies and Claims ---
        POLICY_TYPES = ['dental', 'life', 'disability', 'car']
        START_DATE = date(2023, 1, 1)
        END_DATE = date(2025, 10, 12)
        DATE_RANGE_DAYS = (END_DATE - START_DATE).days

        family_list = []
        members_list = []
        policies_list = []
        claims_list = []
        relationships_list = []

        claim_id_counter = 1
        summary_fam_label = None # Will hold the fam_label of the first family
        for _ in range(num_families):
            lastname = random.choice(LAST_NAMES)
            fam_id = f"{random.randint(0, 999999):06}"
            fam_label = f"{lastname}_{fam_id}"

            if summary_fam_label is None:
                summary_fam_label = fam_label

            family_list.append({
                'fam_id': fam_id,
                'lastname': lastname,
                'fam_label': fam_label
            })

            current_adults = []
            current_children = []
            used_child_firstnames = set()

            # -- Generate Members and "member_of" Relationships --
            num_parents = random.randint(1, 2)
            for _ in range(num_parents):
                sex = random.choice(SEXES)
                firstname = random.choice(MALE_FIRST_NAMES if sex == 'M' else FEMALE_FIRST_NAMES)
                age = random.randint(22, 55)
                current_adults.append({'firstname': firstname, 'age': age})
                members_list.append({
                    'fam_id': fam_id, 'lastname': lastname, 'firstname': firstname,
                    'adult_Y_N': 'Y', 'sex': sex, 'age': age,
                    'fam_label': fam_label
                })
                relationships_list.append({
                    'fam_id': fam_id,
                    'lastname': lastname,
                    'firstname': firstname,
                    'relationship_name': 'member_of',
                    'vertex1': fam_label,
                    'v1type': 'family',
                    'vertex2': f'{firstname}_{lastname}_{age}_{fam_id}',
                    'v2type': 'member',
                    'fam_label': fam_label
                })

            # -- Generate "has_spouse" Relationship Record --
            if len(current_adults) > 1:
                adult1 = current_adults[0]
                adult2 = current_adults[1]
                relationships_list.append({
                    'fam_id': fam_id,
                    'lastname': lastname,
                    'adult1_firstname': adult1['firstname'],
                    'adult2_firstname': adult2['firstname'],
                    'relationship_name': 'has_spouse',
                    'vertex1': f"{adult1['firstname']}_{lastname}_{adult1['age']}_{fam_id}",
                    'v1type': 'member',
                    'vertex2': f"{adult2['firstname']}_{lastname}_{adult2['age']}_{fam_id}",
                    'v2type': 'member',
                    'fam_label': fam_label
                })

            num_children = random.randint(0, 5)
            for _ in range(num_children):
                sex = random.choice(SEXES)
                name_list = MALE_FIRST_NAMES if sex == 'M' else FEMALE_FIRST_NAMES

                while True:
                    firstname = random.choice(name_list)
                    if firstname not in used_child_firstnames:
                        used_child_firstnames.add(firstname)
                        break

                age = random.randint(1, 22)
                current_children.append({'firstname': firstname, 'age': age})
                members_list.append({
                    'fam_id': fam_id, 'lastname': lastname, 'firstname': firstname,
                    'adult_Y_N': 'N', 'sex': sex, 'age': age,
                    'fam_label': fam_label
                })
                relationships_list.append({
                    'fam_id': fam_id,
                    'lastname': lastname,
                    'firstname': firstname,
                    'relationship_name': 'member_of',
                    'vertex1': fam_label,
                    'v1type': 'family',
                    'vertex2': f'{firstname}_{lastname}_{age}_{fam_id}',
                    'v2type': 'member',
                    'fam_label': fam_label
                })

            # -- Generate "has_child" Relationship Records --
            for adult in current_adults:
                for child in current_children:
                    relationships_list.append({
                        'fam_id': fam_id,
                        'lastname': lastname,
                        'adult_firstname': adult['firstname'],
                        'child_firstname': child['firstname'],
                        'relationship_name': 'has_child',
                        'vertex1': f"{adult['firstname']}_{lastname}_{adult['age']}_{fam_id}",
                        'v1type': 'member',
                        'vertex2': f"{child['firstname']}_{lastname}_{child['age']}_{fam_id}",
                        'v2type': 'member',
                        'fam_label': fam_label
                    })

            # -- Generate Policies and "has_policy" Relationships --
            for policy_type in POLICY_TYPES:
                if random.choice([True, False]):
                    policy_name = f"{fam_id}_{policy_type}"
                    policies_list.append({
                        'fam_id': fam_id, 'policy_name': policy_name,
                        'lastname': lastname, 'type': policy_type,
                        'fam_label': fam_label
                    })

                    relationships_list.append({
                        'fam_id': fam_id,
                        'policy_name': policy_name,
                        'relationship_name': 'has_policy',
                        'vertex1': fam_label,
                        'v1type': 'family',
                        'vertex2': policy_name,
                        'v2type': 'policy',
                        'fam_label': fam_label
                    })

                    # -- Generate Claims and "has_claim" Relationships --
                    num_claims = random.randint(0, 3)
                    for _ in range(num_claims):
                        claim_amount = round(random.uniform(50.00, 7500.00), 2)
                        random_days_offset = random.randrange(DATE_RANGE_DAYS)
                        claim_date = START_DATE + timedelta(days=random_days_offset)

                        claims_list.append({
                            'claim_id': claim_id_counter, 'policy_name': policy_name,
                            'claim_amount': claim_amount, 'claim_date': claim_date.isoformat(),
                            'fam_label': fam_label
                        })

                        relationships_list.append({
                            'policy_name': policy_name,
                            'claim_id': claim_id_counter,
                            'relationship_name': 'has_claim',
                            'vertex1': policy_name,
                            'v1type': 'policy',
                            'vertex2': f'{policy_name}_{claim_id_counter}',
                            'v2type': 'claim',
                            'fam_label': fam_label
                        })

                        claim_id_counter += 1

        node_count = len(family_list) + len(members_list) + len(policies_list) + len(claims_list)
        edge_count = len(relationships_list)

        return {
            'summary': {
                'node_count': node_count,
                'edge_count': edge_count,
                'fam_label': summary_fam_label
            },
            'family': {
                'name': 'family',
                'count': len(family_list),
                'records': family_list
            },
            'members': {
                'name': 'members',
                'count': len(members_list),
                'records': members_list
            },
            'policies': {
                'name': 'policies',
                'count': len(policies_list),
                'records': policies_list
            },
            'claims': {
                'name': 'claims',
                'count': len(claims_list),
                'records': claims_list
            },
            'relationships': {
                'name': 'relationships',
                'count': len(relationships_list),
                'records': relationships_list
            }
        }
