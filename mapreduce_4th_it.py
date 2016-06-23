import settings
from multiprocessing import Process
import os
import json


class FileHandler(object):
    """
    """
    def __init__(self, input_file_path, output_dir):
        """
        Note: the input file path should be given for splitting.
        The output directory is needed for joining the outputs.

        :param input_file_path: input file path
        :param output_dir: output directory path

        """
        self.input_file_path = input_file_path
        self.output_dir = output_dir

    def begin_file_split(self, split_index, index):
        """initialize a split file by opening and adding an index.

        :param split_index: the split index we are currently on, to be used for naming the file.
        :param index: the index given to the file.

        """
        file_split = open(settings.get_input_split_file(split_index-1), "w+")
        file_split.write(str(index) + "\n")
        return file_split

    def is_on_split_position(self, character, index, split_size, current_split):
        """Check if it is the right time to split.
        i.e: character is a space and the limit has been reached.

        :param character: the character we are currently on.
        :param index: the index we are currently on.
        :param split_size: the size of each single split.
        :param current_split: the split we are currently on.

        """
        return index>split_size*current_split+1 and character.isspace()

    def split_file(self, number_of_splits):
        """split a file into multiple files.
        note: this has not been optimized to avoid overhead.

        :param number_of_splits: the number of chunks to
        split the file into.

        """
        file_size = os.path.getsize(self.input_file_path)
        unit_size = file_size / number_of_splits + 1
        original_file = open(self.input_file_path, "r")
        file_content = original_file.read()
        original_file.close()
        (index, current_split_index) = (1, 1)
        current_split_unit = self.begin_file_split(current_split_index, index)
        for character in file_content:
            current_split_unit.write(character)
            if self.is_on_split_position(character, index, unit_size, current_split_index):
                current_split_unit.close()
                current_split_index += 1
                current_split_unit = self.begin_file_split(current_split_index,index)
            index += 1
        current_split_unit.close()

    def join_files(self, number_of_files, clean = False, sort = True, decreasing = True):
        """join all the files in the output directory into a
        single output file.

        :param number_of_files: total number of files.
        :param clean: if True the reduce outputs will be deleted,
        by default takes the value of self.clean.
        :param sort: sort the outputs.
        :param decreasing: sort by decreasing order, high value
        to low value.

        :return output_join_list: a list of the outputs
        """
        output_join_list = []
        for reducer_index in xrange(0, number_of_files):
            f = open(settings.get_output_file(reducer_index), "r")
            output_join_list += json.load(f)
            f.close()
            if clean:
                os.unlink(settings.get_output_file(reducer_index))
        if sort:
            from operator import itemgetter as operator_ig
            # sort using the key
            output_join_list.sort(key=operator_ig(1), reverse=decreasing)
        output_join_file = open(settings.get_output_join_file(self.output_dir), "w+")
        json.dump(output_join_list, output_join_file)
        output_join_file.close()
        return output_join_list


class MapReduce(object):
    """MapReduce class representing the mapreduce model

    note: the 'mapper' and 'reducer' methods must be
    implemented to use the mapreduce model.
    """
    def __init__(self, input_dir = settings.default_input_dir, output_dir = settings.default_output_dir,
                 n_mappers = settings.default_n_mappers, n_reducers = settings.default_n_reducers,
                 clean = True):
        """

        :param input_dir: directory of the input files,
        taken from the default settings if not provided
        :param output_dir: directory of the output files,
        taken from the default settings if not provided
        :param n_mappers: number of mapper threads to use,
        taken from the default settings if not provided
        :param n_reducers: number of reducer threads to use,
        taken from the default settings if not provided
        :param clean: optional, if True temporary files are
        deleted, True by default.
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers
        self.clean = clean

    def mapper(self, key, value):
        """outputs a list of key-value pairs, where the key is
        potentially new and the values are of a potentially different type.
        Note: this function is to be implemented.

        :param key:
        :param value:
        note: this method should be implemented
        """
        pass

    def reducer(self, key, values_list):
        """Outputs a single value together with the provided key.
        Note: this function is to be implemented.

        :param key:
        :param value_list:
        note: this method should be implemented
        """
        pass

    def run_mapper(self, index):
        """Runs the implemented mapper

        :param index: the index of the thread to run on
        """
        pass

    def run_reducer(self, index):
        """Runs the implemented reducer

        :param index: the index of the thread to run on
        """
        pass

    def run(self):
        """Executes the map and reduce operations

        """
        # initialize mappers list
        map_workers = []
        # initialize reducers list
        rdc_workers = []
        # run the map step
        for thread_id in range(self.n_mappers):
            p = Process(target=self.run_mapper, args=(thread_id,))
            p.start()
            map_workers.append(p)
        [t.join() for t in map_workers]
        # run the reduce step
        for thread_id in range(self.n_reducers):
            p = Process(target=self.run_reducer, args=(thread_id,))
            p.start()
            map_workers.append(p)
        [t.join() for t in rdc_workers]