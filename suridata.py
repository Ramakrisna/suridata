import random
from itertools import cycle
from multiprocessing import Pool, cpu_count
from typing import Optional, Dict


class CreateGame:
    def __init__(self, employees_list: list, workers: int = cpu_count()):
        self._employees = employees_list
        self.employee_tuples: Optional[list] = None
        self.validate_employees()
        self._results: dict = {}
        self.create_game(workers)

    def validate_employees(self):
        """
        A method that takes the list of employees and validates that the input is for each employee is valid and that
        there aren't any duplicates
        """
        for employee in self._employees[::-1]:
            if not (isinstance(employee, dict) and len(employee.values()) == 3
                    and isinstance(employee['department'], str) and isinstance(employee['name'], str)
                    and (isinstance(employee.get('age'), int) or
                         isinstance(employee.get('age'), str) and employee.get('age').isdigit())):
                self._employees.remove(employee)
                continue
            for field in ('department', 'name'):
                employee[field] = employee[field].lower()
        self.employee_tuples = list(set(tuple(employee.values()) for employee in self._employees))

    def validate_giant(self, dwarf: tuple, giant: tuple, results: dict) -> bool:
        """
        Validate that the giant and the dwarf can be paired and that they're not crossing any of the game rules
        """
        if (giant in results and results[giant] == dwarf) or giant in results.values() or dwarf == giant:
            return False
        return True

    @staticmethod
    def split_list_to_chunks(items: list, chunks: int) -> list:
        """
        A method that takes a list of employees and divides it into roughly even sub lists
        """
        split_list = [[] for _ in range(min(chunks, len(items)))]
        for item, chunk in zip(items, cycle(split_list)):
            chunk.append(item)
        return split_list

    def randomize_chunks(self, employees_chunk: list) -> Dict[tuple, tuple]:
        """
        A method to create random tuples of employees from a chunk of employees
        """
        results = {}
        for dwarf_idx in range(len(employees_chunk)):
            validate = False
            while not validate:
                giant_idx = random.randrange(len(employees_chunk))
                validate = self.validate_giant(employees_chunk[dwarf_idx], employees_chunk[giant_idx], results)
                if validate:
                    results[employees_chunk[dwarf_idx]] = employees_chunk[giant_idx]
        return results

    def create_game(self, chunks: int) -> None:
        if len(self.employee_tuples) < 3:
            raise ValueError('Not enough employees to play the game')
        random.shuffle(self.employee_tuples)
        split_list = self.split_list_to_chunks(self.employee_tuples, chunks)
        with Pool(processes=chunks) as pool:
            results = pool.map(self.randomize_chunks, split_list)
        flatten_results = []
        for result in results:
            flatten_results += list(result.items())
        print([(result[0][1], result[1][1]) for result in flatten_results])


if __name__ == '__main__':
    employees = [
        {'department': 'R&D', 'name': 'Nikolas Porter', 'age': 46},
        {'department': 'Sales', 'name': 'Sterling Walton', 'age': 28},
        {'department': 'R&D', 'name': 'Louis Mcintosh', 'age': 33},
        {'department': 'R&D', 'name': 'Joyce Randolph', 'age': 29},
        {'department': 'R&D', 'name': 'Oliver Mcconnell', 'age': 63},
        {'department': 'Support', 'name': 'Jimena Roman', 'age': 22},
        {'department': 'Support', 'name': 'Ellis Davenport', 'age': 25},
        {'department': 'Sales', 'name': 'Jorge Good', 'age': 29},
        {'department': 'R&D', 'name': 'Sasha Horn', 'age': 50},
        {'department': 'Sales', 'name': 'Aracely Nguyen', 'age': 63},
        {'department': 'Support', 'name': 'Kenyon York', 'age': 71},
        {'department': 'R&D', 'name': 'Oliver Mcconnell', 'age': 63},
        {'department': 'Sales', 'name': 'Saniyah Luna', 'age': 27},
        {'department': 'R&D', 'name': 'Abigayle Sosa', 'age': 29},
        {'department': 'R&D', 'name': 'Elisha Andrade', 'age': 24},
        {'department': 'Sales', 'name': 'Abril Schaefer', 'age': 22},
        {'department': 'R&D', 'name': 'Katrina Hanna', 'age': 21},
        {'department': 'R&D', 'name': 'Kaiya Fry', 'age': 28},
        {'department': 'R&D', 'name': 'Shyann Harmon', 'age': 33},
        {'department': 'R&D', 'name': 'Darnell Rangel', 'age': 31},
        {'department': 'R&D', 'name': 'Kendall Cochran', 'age': 37},
        {'department': 'R&D', 'name': 'Kylan Cantrell', 'age': 28},
        {'department': 'R&D', 'name': 'Amiah Powell', 'age': 29},
        {'department': 'R&D', 'name': 'Maria Kelley', 'age': 22},
        {'department': 'R&D', 'name': 'Jay Gonzales', 'age': 43},
        {'department': 'R&D', 'name': 'Shea Robles', 'age': 29},
        {'department': 'R&D', 'name': 'Kenyon Patel', 'age': 23},
        {'department': 'Support', 'name': 'Esmeralda Harris', 'age': 33},
        {'department': 'Sales', 'name': 'Donovan Petersen', 'age': 23},
        {'department': 'Sales', 'name': 'Ralph Yu', 'age': 25},
        {'department': 'R&D', 'name': 'Nadia Hernandez', 'age': 26},
        {'department': 'Support', 'name': 'Finley Vang', 'age': 27},
        {'department': 'R&D', 'name': 'Kelvin Cameron', 'age': 44},
        {'department': 'Support', 'name': 'Zack Barnes', 'age': 23},
        {'department': 'R&D', 'name': 'Evelyn Roth', 'age': 23},
        {'department': 'Sales', 'name': 'Charlize Cobb', 'age': 47},
        {'department': 'Sales', 'name': 'Devin Benitez', 'age': 23},
        {'department': 'R&D', 'name': 'Jaidyn Noble', 'age': 70},
        {'department': 'Sales', 'name': 'Jaydin Braun', 'age': 63},
        {'department': 'Sales', 'name': 'Anthony Bray', 'age': 51},
        {'department': 'Sales', 'name': 'Leila Becker', 'age': 55},
        {'department': 'R&D', 'name': 'Simon Walker', 'age': 57},
        {'department': 'R&D', 'name': 'Alan Kemp', 'age': 22},
        {'department': 'R&D', 'name': 'Elliot Cantu', 'age': 23},
        {'department': 'Support', 'name': 'Fiona Pearson', 'age': 25},
        {'department': 'Sales', 'name': 'Clara Bradley', 'age': 23},
        {'department': 'R&D', 'name': 'Aimee Mcpherson', 'age': 38},
        {'department': 'R&D', 'name': 'Quinn Reese', 'age': 43},
        {'department': 'Sales', 'name': 'Houston Nguyen', 'age': 33},
        {'department': 'Support', 'name': 'Jayleen Henry', 'age': 37},
        {'department': 'Sales', 'name': 'Terrance Gallagher', 'age': 23},
        {'department': 'R&D', 'name': 'Axel Bolton', 'age': 23},
        {'department': 'Sales', 'name': 'Dario Robertson', 'age': 23},
        {'department': 'Sales', 'name': 'Nadia David', 'age': 82},
        {'department': 'Support', 'name': 'Grayson Barrera', 'age': 30},
        {'department': 'Support', 'name': 'Lillie Pollard', 'age': 30},
        {'department': 'R&D', 'name': 'Desiree Carey', 'age': 30},
        {'department': 'R&D', 'name': 'Jaden Hardin', 'age': 30},
        {'department': 'Sales', 'name': 'Jason Jimenez', 'age': 30},
        {'department': 'Sales', 'name': 'Jordyn May', 'age': 32},
        {'department': 'Sales', 'name': 'Alvaro Haley', 'age': 33},
        {'department': 'Sales', 'name': 'Zackary Nguyen', 'age': 54},
        {'department': 'Support', 'name': 'Lilliana Wood', 'age': 42},
        {'department': 'Support', 'name': 'Koen Luna', 'age': 24},
        {'department': 'Sales', 'name': 'Taniyah Ramos', 'age': 21},
        {'department': 'Sales', 'name': 'Messiah Glover', 'age': 20},
        {'department': 'R&D', 'name': 'Jazmine Massey', 'age': 29},
        {'department': 'Support', 'name': 'Carolina Rowe', 'age': 28},
        {'department': 'Sales', 'name': 'Heaven Bartlett', 'age': 26},
        {'department': 'Sales', 'name': 'Harry Peterson', 'age': 44},
        {'department': 'Sales', 'name': 'Francisco Escobar', 'age': 69},
        {'department': 'Support', 'name': 'Brendon Osborne', 'age': 65},
        {'department': 'Support', 'name': 'Yuram Henson', 'age': 55},
        {'department': 'R&D', 'name': 'Lindsey Hines', 'age': 48},
        {'department': 'R&D', 'name': 'Brayden Young', 'age': 35},
        {'department': 'Support', 'name': 'Rhianna Potter', 'age': 30},
        {'department': 'Support', 'name': 'June Hanna', 'age': 19},
        {'department': 'R&D', 'name': 'Eli Buck', 'age': 20},
        {'department': 'R&D', 'name': 'Shayna Burke', 'age': 65},
        {'department': 'Support', 'name': 'Kristopher Sanders', 'age': 62},
        {'department': 'R&D', 'name': 'Tate Yates', 'age': 52},
        {'department': 'Sales', 'name': 'Hayden Massey', 'age': 50},
        {'department': 'R&D', 'name': 'Grady Baird', 'age': 51},
        {'department': 'Sales', 'name': 'Dalia Gomez', 'age': 44},
        {'department': 'Support', 'name': 'Riley Fowler', 'age': 48},
        {'department': 'R&D', 'name': 'Madilyn Melton', 'age': 37},
        {'department': 'R&D', 'name': 'Omar Browning', 'age': 39},
        {'department': 'R&D', 'name': 'Melody Nielsen', 'age': 44},
        {'department': 'R&D', 'name': 'Camron Jacobs', 'age': 30},
        {'department': 'R&D', 'name': 'Royce Moore', 'age': 41},
        {'department': 'Support', 'name': 'Ariel Reed', 'age': 42},
        {'department': 'Support', 'name': 'Yuram Henson', 'age': 55},
        {'department': 'Sales', 'name': 'Kaleb Benitez', 'age': 51},
        {'department': 'Sales', 'name': 'Mariyah Park', 'age': 52},
        {'department': 'Sales', 'name': 'Saige Castro', 'age': 40},
        {'department': 'Sales', 'name': 'Kristin Dorsey', 'age': 61},
        {'department': 'Support', 'name': 'Reed Parks', 'age': 62},
        {'department': 'R&D', 'name': 'Gerald Booker', 'age': 43},
        {'department': 'Support', 'name': 'Bennett Wolf', 'age': 23},
        {'department': 'Support', 'name': 'Abby Zuniga', 'age': 30},
        {'department': 'R&D', 'name': 'Vaughn Phelps', 'age': 19},
        {'department': 'R&D', 'name': 'Aditya Wilkerson', 'age': 27},
        {'department': 'Support', 'name': 'Jadon Tucker', 'age': 26},
        {'department': 'Sales', 'name': 'Erica Bullock', 'age': 34},
        {'department': 'Sales', 'name': 'Wilson Medina', 'age': 36},
        {'department': 'Support', 'name': 'Justice Boyle', 'age': 37},
        {'department': 'Support', 'name': 'Mina Caldwell', 'age': 44}
    ]
    game = CreateGame(employees_list=employees, workers=2)
