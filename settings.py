# set default directory for the input files
DEFAULT_INP_DIR = "mapreduce/input_files"
# set default directory for the temporary map files
DEFAULT_MAP_DIR = "mapreduce/temp_map_files"
# set default directory for the output files
DEFAULT_OUT_DIR = "mapreduce/output_files"
# set default number for the map and reduce threads
DEFAULT_N_MAP = 4
DEFAULT_N_RED = 4

# return the name of the input file to be split into chunks
def get_input_file(input_dir = None, extension = ".ext"):
    if not(input_dir == None):
        return input_dir+"/file" + extension
    return DEFAULT_INP_DIR+"/file" + extension
    
# return the name of the current split file corresponding to the given index
def get_input_split_file(index, input_dir = None, extension = ".ext"):
    if not(input_dir == None):
        return input_dir+"/file_"+ str(index) + extension
    return DEFAULT_INP_DIR+"/file_"+ str(index) + extension
        
# return the name of the temporary map file corresponding to the given index
def get_temp_map_file(index, reducer, output_dir = None, extension = ".ext"):
    if not(output_dir == None):
        return output_dir +"/map_file_" + str(index)+"-" + str(reducer) + extension
    return DEFAULT_OUT_DIR+"/map_file_" + str(index)+"-"+ str(reducer) + extension


#def get_temp_map_file(index, output_dir = None, extension = ".ext"):
 #   if not(output_dir == None):
  #      return output_dir+"/"+DEFAULT_MAP_DIR+"/file_"+ index + # return DEFAULT_MAP_DIR+"/file_"+ index + extension

# return the name of the output file given its corresponding index
def get_output_file(index, output_dir = None, extension = ".out"):
    if not(output_dir == None):
        return output_dir+"/reduce_file_"+ str(index) + extension
    return DEFAULT_OUT_DIR+"/reduce_file_"+ str(index) + extension
    
    
# return the name of the output file
def get_output_join_file(output_dir = None, extension = ".out"):
    if not(output_dir == None):
        return output_dir +"/output" + extension
    return DEFAULT_OUT_DIR+"/output" + extension    
