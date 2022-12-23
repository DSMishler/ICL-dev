# Daniel Mishler
# Last push to github 2022-09-26

# TODO: should I sort the tasks before parsing them *within* a frame?
# Just because I haven't seen a bug doesn't mean it's not possiblle...

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from IPython import display
import numpy as np
import math
import time
import xmltodict

'''
What the deal with order generators?
When using task insertion through dtd, the trace does not track data such as the m, n, and k tiles.
Because of this, we need to take one extra step to know what order the tasks were generated in
(since they are monotonically sorted by id) to extract this information from the trace.
'''

def rectangle_order_generator_improved(Ntiles_m, Ntiles_n=None, Ntiles_k=None, square_size = 6):
    # assumes square matrix which takes N tiles to cover
    i = 0 # parsing through m
    j = 0 # parsing through n
    k = 0
    if Ntiles_n is None:
        Ntiles_n = Ntiles_m
    if Ntiles_k is None:
        Ntiles_k = Ntiles_m
    checkpoint_i = 0
    checkpoint_j = 0
    ordered_list = []
    while(checkpoint_i < Ntiles_m):
        checkpoint_j = 0
        while(checkpoint_j < Ntiles_n):
            k = 0
            while(k < Ntiles_k):
                i = checkpoint_i
                while(i < checkpoint_i + square_size and i < Ntiles_m):
                    j = checkpoint_j
                    while(j < checkpoint_j + square_size and j < Ntiles_n):
                        ordered_list.append([i,j,k])
                        j += 1
                    i += 1
                k += 1
            k = Ntiles_k - 1 #k--
            while(k >= 0):
                i = checkpoint_i+square_size
                while(i < checkpoint_i + 2*square_size and i < Ntiles_m):
                    j = checkpoint_j
                    while(j < checkpoint_j + square_size and j < Ntiles_n):
                        ordered_list.append([i,j,k])
                        j += 1
                    i += 1
                k -= 1
            k = 0
            while(k < Ntiles_k):
                i = checkpoint_i+square_size
                while(i < checkpoint_i + 2*square_size and i < Ntiles_m):
                    j = checkpoint_j + square_size
                    while(j < checkpoint_j + 2*square_size and j < Ntiles_n):
                        ordered_list.append([i,j,k])
                        j += 1
                    i += 1
                k += 1
            k = Ntiles_k - 1 #k--
            while(k >= 0):
                i = checkpoint_i
                while(i < checkpoint_i + square_size and i < Ntiles_m):
                    j = checkpoint_j + square_size
                    while(j < checkpoint_j + 2*square_size and j < Ntiles_n):
                        ordered_list.append([i,j,k])
                        j += 1
                    i += 1
                k -= 1
            checkpoint_j += (2 * square_size)
        checkpoint_i += (2 * square_size)
    return ordered_list

def rectangle_order_generator_naive(Ntiles_m, Ntiles_n=None, Ntiles_k=None, square_size = 6):
    # assumes square matrix which takes N tiles to cover
    i = 0 # parsing through m
    j = 0 # parsing through n
    k = 0
    if Ntiles_n is None:
        Ntiles_n = Ntiles_m
    if Ntiles_k is None:
        Ntiles_k = Ntiles_m
    checkpoint_i = 0
    checkpoint_j = 0
    ordered_list = []
    while(checkpoint_i < Ntiles_m):
        checkpoint_j = 0
        while(checkpoint_j < Ntiles_n):
            k = 0
            while(k < Ntiles_k):
                i = checkpoint_i
                while(i < checkpoint_i + square_size and i < Ntiles_m):
                    j = checkpoint_j
                    while(j < checkpoint_j + square_size and j < Ntiles_n):
                        ordered_list.append([i,j,k])
                        j += 1
                    i += 1
                k += 1
            k = Ntiles_k - 1 #k--
            while(k >= 0):
                i = checkpoint_i+square_size
                while(i < checkpoint_i + 2*square_size and i < Ntiles_m):
                    j = checkpoint_j
                    while(j < checkpoint_j + square_size and j < Ntiles_n):
                        ordered_list.append([i,j,k])
                        j += 1
                    i += 1
                k -= 1
            checkpoint_j += square_size
        checkpoint_i += (2 * square_size)
    return ordered_list


def sequential_order_generator(Ntiles_m, Ntiles_n, Ntiles_k, square_size = 6):
    i = 0 # parsing through m
    j = 0 # parsing through n
    k = 0
    ordered_list = []
    while(i < Ntiles_m):
        j = 0
        while(j < Ntiles_n):
            k = 0
            while(k < Ntiles_k):
                ordered_list.append([i,j,k])
                k += 1
            j += 1
        i += 1
        
    return ordered_list


# rogw: recursive order generator work
def rogw(square_size, min_m, max_m, min_n, max_n, min_k, max_k):
    ordered_list = []
    mspace = max_m - min_m
    nspace = max_n - min_n
    kspace = max_k - min_k
    if mspace <= square_size and nspace <= square_size and kspace <= square_size:
        for m in range(min_m, max_m):
            for n in range(min_n, max_n):
                for k in range(min_k, max_k):
                    ordered_list.append([m,n,k])
    elif mspace > square_size and nspace > square_size and kspace > square_size:
        heirarchy_list = []
        heirarchy_list.append(rogw(square_size, min_m, max_m - mspace//2, min_n, max_n - nspace//2, min_k, max_k - kspace//2))
        heirarchy_list.append(rogw(square_size, min_m, max_m - mspace//2, min_n, max_n - nspace//2, max_k - kspace//2, max_k))
        heirarchy_list.append(rogw(square_size, min_m, max_m - mspace//2, max_n - nspace//2, max_n, min_k, max_k - kspace//2))
        heirarchy_list.append(rogw(square_size, min_m, max_m - mspace//2, max_n - nspace//2, max_n, max_k - kspace//2, max_k))
        heirarchy_list.append(rogw(square_size, max_m - mspace//2, max_m, min_n, max_n - nspace//2, min_k, max_k - kspace//2))
        heirarchy_list.append(rogw(square_size, max_m - mspace//2, max_m, min_n, max_n - nspace//2, max_k - kspace//2, max_k))
        heirarchy_list.append(rogw(square_size, max_m - mspace//2, max_m, max_n - nspace//2, max_n, min_k, max_k - kspace//2))
        heirarchy_list.append(rogw(square_size, max_m - mspace//2, max_m, max_n - nspace//2, max_n, max_k - kspace//2, max_k))
        for sublist in heirarchy_list:
            for element in sublist:
                ordered_list.append(element)
    else:
        print("nonsquare matrices detected and this code isn't developed. You're pushin' yourself too hard, Danny.")
    return ordered_list
    

def recursive_order_generator(Ntiles_m, Ntiles_n, Ntiles_k, square_size = 8):
    ordered_list = rogw(square_size, 0, Ntiles_m, 0, Ntiles_n, 0, Ntiles_k)
    return ordered_list



################################################################################################
# helper functions to make the animation function itself shorter and more readable
################################################################################################
def check_parameters_basic(M, N, K, tilesize, fill, which_animate):
    if M is None:
        print("Error: must provide argument for M")
        return True
    if N is None:
        print("Error: must provide argument for N")
        return True
    if K is None:
        print("Error: must provide argument for K (even if you don't think it makes sense in the problem!)")
        return True
    if tilesize is None:
        print("Error: must provide argument for tilesize (even if you don't think it makes sense in the problem!)")
        return True
    legal_fills = ["relative", "absolute"]
    if fill not in legal_fills:
        print("Error: fill must be in", legal_fills)
        return True
    legal_animations = ["tasks", "abctasks", "progress", "abcprogress", "coreswaps"]
    if which_animate not in legal_animations:
        print("Error: `which_animate` parameter must be in", legal_animations)
        return True
    return False

def guess_trace_type(trace):
    # TODO: Make a more sophisticated guess
    try:
        trace.events["m"]
        trace_type = "ptg"
    except KeyError:
        trace_type = "dtd"
    return trace_type

def guess_running_system(trace):
    # TODO: Make a more sophisticated guess
    try:
        trace.information["HWLOC-XML"]
        running_system = "dplasma"
    except KeyError:
        running_system = "hicma"
    return running_system

def check_for_order_function(trace_type, order_func):
    if trace_type == "ptg":
        error = False
    elif trace_type == "dtd":
        if order_func is None:
            print("Error: for DTD traces, you must supply an order function!")
            error = True
        else:
            error = False
    else:
        print("Error: unknown trace type '" + str(trace_type) + "'")
        error = True
    return error

def check_parameter_compatibility(task_type, trace_type, which_animate, M, N, K):
    if task_type == "gemm":
        error = False
    elif task_type == "potrf":
        error = False
        if trace_type != "ptg":
            print("Error: potrf only supported with ptg")
            error = True
        if(which_animate == "abcprogress" or which_animate == "abctasks"):
            print("Error: potrf only supported with C view")
            error = True
        if N != M:
            print("Error: only square animations of potrf supported")
            error = True
    else:
        print("Error: unknown task type '" + str(task_type) +"'")
        error = True
    return error
    
def get_work_tasks_indices(trace, task_type, running_system):
    # TODO: Feels like this one could use some generalization and cleanup too.
    name_to_task_num = {}
    if task_type == "gemm":
        gemm_index_found = 0
        for name in trace.event_types.index: # An array of numbers indexed by their name. Interesting, huh?
            if "gemm" in name.lower():
                name_to_task_num["gemm"] = trace.event_types[name]
                gemm_index_found += 1
                break
        if gemm_index_found != 1:
            print("Error: file trace does not have its event_types set properly")
            print("found %d events, expected %d" % (gemm_index_found, 1))
            return None
    
    elif task_type == "potrf":
        potrf_index_found = 0
        for name in trace.event_types.index:
            if "potrf_" in name.lower(): # Could possibly use regex for this...
                for taskname in ["gemm", "syrk", "trsm", "potrf"]:
                    if(taskname in name.split('_')[-1].lower()):
                        print("found task type %s (#%d)" % (name.lower(), trace.event_types[name]))
                        if(running_system == "hicma"):
                            if("3flow" in name.lower()):
                                name_to_task_num["large-"+taskname] = trace.event_types[name]
                            else:
                                name_to_task_num["small-"+taskname] = trace.event_types[name]
                        elif(running_system == "dplasma"):
                            name_to_task_num[taskname] = trace.event_types[name]
                        else:
                            print("error: unknown running system")
                        potrf_index_found += 1
                        break
        if(running_system == "hicma"):
            expected_task_types = 8
        elif(running_system == "dplasma"):
            expected_task_types = 4
        if potrf_index_found != expected_task_types:
            print("Error: file trace does not have its event_types set properly")
            print("found %d events, expected %d" % (potrf_index_found, 4))
            return None
    return name_to_task_num

def work_tasks_indices_from_names_dict(name_to_task_num):
    work_tasks_indices = []
    for key in name_to_task_num:
        work_tasks_indices.append(name_to_task_num[key])
    return work_tasks_indices
    
def get_potrf_uplo(running_system, orderdf, name_to_task_num):
    # TODO: There gotta be a better way to do this than peek at a task, no?
    if running_system == "dplasma":
        test_task = "trsm"
    elif running_system == "hicma":
        test_task = "small-trsm"
    else:
        print("Error, unknown running system!")
        return None
    
    if orderdf.loc[orderdf["type"] == name_to_task_num[test_task]].iloc[0]["m"] is None:
        potrf_uplo = "upper"
    else:
        potrf_uplo = "lower"
    return potrf_uplo

def get_time_per_frame(first_begin, last_end, num_frames, enforce_interval):
    # Possibly enforce an interval in seconds
    if enforce_interval is not None:
        time_per_frame = enforce_interval
        num_frames = math.ceil((last_end - first_begin)/10**9/time_per_frame)
        print("enforcing %d frames to grant you your requested enforced interval" % (num_frames))
    if(num_frames <= 0):
        print("Error: illegal number of frames. Must be at least 1")
        return None
    time_per_frame = (last_end - first_begin)/10**9/num_frames
    return time_per_frame

def get_estimated_video_time(which_animate, running_system, task_type, num_frames, Ntiles_m, Ntiles_n):
    # Estimate time to get graphs
    # TODO: find a better way to calculate this - currently it's just loosely
    #       fit to whatever I observe on my laptop.
    estimation_multiplier = 1
    if(which_animate == "abcprogress" or which_animate == "abctasks"):
        estimation_multiplier *= 2 # for abc type calls
    if running_system == "hicma":
        estimation_multiplier *= 0.20
    if task_type == "gemm":
        estimation_multiplier *= 3
    estimated_compile_time = (num_frames * 0.0004 * ((Ntiles_m * Ntiles_n)**0.9) * estimation_multiplier)
    return estimated_compile_time

def get_expected_tasks(task_type, running_system,
                       Ntiles_m, Ntiles_n, Ntiles_k,
                       tilesize, bigtilesize):
    # How many work tasks should I expect, given what I know about the problem?
    # This information will prove useful for error checking.
    if task_type == "gemm":
        expected_tasks = Ntiles_m * Ntiles_n * Ntiles_k
    elif task_type == "potrf":
        if Ntiles_m != Ntiles_n:
            print("error: expected 'n' and 'm' to be equal to POTRF trace")
            return None
        # expected_tasks = Ntiles_m # potrf
        # expected_tasks += ((Ntiles_m-1)*(Ntiles_m)) // 2 # trsm
        # expected_tasks += ((Ntiles_m-1)*(Ntiles_m)) // 2 # syrk
        # for i in range(Ntiles_m-1): # gemm 
            # expected_tasks += (i*(i+1)) // 2
        # Alternative methods
        # n * 1 + (n-1) * 2 + (n-2) * 3 + (n-3) * 4 + ...
        # which is same as: sum over (x^2/2 + x/2) from 1 to n
        # which is the same as: (n) * (n+1) * (n+2) / 6
        if running_system == "dplasma":
            expected_tasks = (Ntiles_m)*(Ntiles_m+1)*(Ntiles_m+2)//6
        elif running_system == "hicma":
            if bigtilesize % tilesize != 0:
                print("error: cannot have large tilesize and smaller tilesize not evenly divide.")
                return None
            small_tiles = bigtilesize//tilesize
            Nbigtiles = math.ceil((Ntiles_m*tilesize)/bigtilesize)
            # large tasks
            expected_tasks = (Nbigtiles)*(Nbigtiles+1)*(Nbigtiles+2)//6
            # small tasks
            tasks_per_potrf = (small_tiles)*(small_tiles+1)*(small_tiles+2)//6
            expected_tasks += Nbigtiles * (tasks_per_potrf)
    return expected_tasks

def get_dtd_helpers(orderdf, order_func, Ntiles_m, Ntiles_n, Ntiles_k):
    id_orders = np.array(orderdf["id"])
    
    # Prepare an list of indices that index which tasks were executing
    # based on the list of the order the tasks were inserted
    indices_arr = np.zeros(len(id_orders),dtype=int)
    check_for = 0
    for id_normed in range(len(indices_arr)):
        while(len(np.where(id_orders == check_for)[0]) == 0):
            check_for += 1
        indices_arr[np.where(id_orders == check_for)[0][0]] = id_normed
        check_for += 1
    # Set up the ideal order given the implementation provided
    ideal_order = order_func(Ntiles_m, Ntiles_n, Ntiles_k)
    return (id_orders, ideal_order, indices_arr)
    
def init_global_arrays(trace, Ntiles_m, Ntiles_n, Ntiles_k, which_animate):
    global A_status
    global B_status
    global C_status
    global E_status
    global core_work_tiles
    global core_migrations
    global migrations_dict
    global cache_sim_dict
    global core_hits
    global sim_memory
    global A_expected # Currently unused
    global B_expected
    global C_expected
    C_status = []
    C_expected = []
    for i in range(Ntiles_m):
        C_status.append(np.zeros(Ntiles_n))
        C_expected.append(np.zeros(Ntiles_n))
    C_status = np.array(C_status)
    C_expected = np.array(C_expected)
    core_work_tiles = []
    num_cores = trace.information["nb_cores"]
    for i in range(num_cores):
        core_work_tiles.append([-1,-1,-1])
    core_work_tiles = np.array(core_work_tiles)
    
    if(which_animate == "abcprogress" or which_animate == "abctasks"):
        A_status = []
        A_expected = []
        for i in range(Ntiles_m):
            A_status.append(np.zeros(Ntiles_k))
            A_expected.append(np.zeros(Ntiles_k))
        A_status = np.array(A_status)
        A_expected = np.array(A_expected)
        B_status = []
        B_expected = []
        for i in range(Ntiles_k):
            B_status.append(np.zeros(Ntiles_n))
            B_expected.append(np.zeros(Ntiles_n))
        B_status = np.array(B_status)
        B_expected = np.array(B_expected)
        E_status = []
        for i in range(Ntiles_m):
            E_status.append(np.zeros(Ntiles_n))
        E_status = np.array(B_status)
    
def get_figsize_dimensions(Ntiles_m, Ntiles_n):
    # TODO: get this to conside Ntiles_k
    fmxd = 6.5 # figsize maximum dimension
    fmnd = 3.5 # figsize minimum dimension
    if Ntiles_m >= Ntiles_n:
        figsize_x = fmxd*Ntiles_n/Ntiles_m
        figsize_y = fmxd
        if(figsize_x < fmnd):
            figsize_x = fmnd
    else:
        figsize_x = fmxd
        figsize_y = fmxd*Ntiles_m/Ntiles_n
        if(figsize_y < fmnd):
            figsize_y = fmnd
    figsize_dims = [figsize_x, figsize_y]
    return figsize_dims

def init_plt_figure(which_animate, figsize_dims):
    if(which_animate == "abcprogress" or which_animate == "abctasks"):
        fig, ((axE, axB), (axA, axC)) = plt.subplots(2,2, figsize = figsize_dims)
        # fig.colorbar(axC.pcolormesh(C_status,vmin=0,vmax=1))
        axA.pcolormesh(A_status, vmin = 0, vmax = 1)
        axA.invert_yaxis()
        axB.pcolormesh(B_status, vmin = 0, vmax = 1)
        axB.invert_yaxis()
        axC.pcolormesh(C_status, vmin = 0, vmax = 1)
        axC.invert_yaxis()
        if(which_animate == "abcprogress"):
            axE.pcolormesh(C_status, vmin = 0, vmax = 1)
            axE.invert_yaxis()
    else:
        fig, axC = plt.subplots(1, figsize = figsize_dims)
        axC.pcolormesh(C_status, vmin = 0, vmax = 1)
        # fig.colorbar(axC.pcolormesh(C_status,vmin=0,vmax=1))
        axC.invert_yaxis()
        axE = None
        axA = None
        axB = None
    return (fig, ((axE, axB), (axA, axC)))


def size_of_tile(tilesize, datatype="double"):
    datatype=datatype.lower()
    if(datatype == "double" or datatype == "d" or datatype == "single precision complex" or datatype == "s"):
        multiplier = 8
    elif(datatype == "float" or datatype == "f"):
        multiplier = 4
    elif(datatype == "complex" or datatype == "z"):
        multiplier = 16
    else:
        print("not supported data type")
        return None
    return tilesize*tilesize*multiplier

class Tile:
    def __init__(self, i, j, k, size, dtype="d"):
        self.m = i
        self.n = j
        self.k = k
        self.id = str(i)+'-'+str(j)+'-'+str(k)
        self.mnid = str(i)+'-'+str(j)
        self.dtype = dtype
        self.size = size_of_tile(size,dtype)

class Cache:
    def __init__(self, name, size, max_residents):
        self.name = name
        self.size = size
        self.residents = [] # a list of tile ids `tileid`s
        self.max_residents = max_residents #precalculated based on tile size
        self.subcaches = []
        self.cores = []     # The cores that this class can service
    def full_access(self, tileid):
        cache_hit = None
        if tileid in self.residents:
            cache_hit = True
            self.residents.remove(tileid)
        else:
            cache_hit = False
            if len(self.residents) == self.max_residents:
                self.residents.remove(self.residents[-1])
        self.residents.insert(0, tileid)
        return cache_hit
    def access(self, tileid):
        cache_hit = None
        if tileid in self.residents:
            cache_hit = True
            # Move it to the front (LRU)
            self.residents.remove(tileid)
            self.residents.insert(0, tileid)
        else:
            cache_hit = False
        return cache_hit
    def insert(self, tileid): # returns: the element that was flushed (if any)
        flushed_element = None
        self.residents.insert(0, tileid)
        if len(self.residents) > self.max_residents:
            flushed_element = self.residents[-1]
            self.residents.remove(flushed_element)
        return flushed_element
    
    def remove(self, elementid): # returns: whether the element was actually removed (from the top-level cache)
                                 # also purges from all lower-level caches
        removed = None
        if elementid in self.residents:
            self.residents.remove(elementid)
            removed = True
        else:
            removed = False
        
        for subcache in self.subcaches:
            subcache.remove(elementid)
            
        return removed
    
    def flush(self):
        self.residents = []

class Memory:
    # has a cache and knows how to access it an all of its sub-parts.
    def __init__(self, highest_level_cache):
        self.cache_top = highest_level_cache
    def access(self, core, m, n, k):
        aid = 'A-'+str(m)+'-'+str(k)
        bid = 'B-'+str(k)+'-'+str(n)
        cid = 'C-'+str(m)+'-'+str(n)
        tileids = [aid, bid, cid]
        access_dict = {}
        # find the lowest level of cache to check first. (assumes only 3 total levels)
        cache_top = self.cache_top
        cache_middle = None
        cache_bottom = None
        for subcache in cache_top.subcaches:
            if core in subcache.cores:
                cache_middle = subcache
                break
        if cache_middle is None:
            print("Cache error!")
            print(f"(seeking core {core})")
            print("available subcaches:")
            for subcache in cache_top.subcaches:
                print(subcache.cores)
            return None
        
        for subcache in cache_middle.subcaches:
            if core in subcache.cores:
                cache_bottom = subcache
                break
        if cache_bottom is None:
            print("Cache error!")
            print(f"(seeking core {core})")
            print("available subcaches:")
            for subcache in cache_middle.subcaches:
                print(subcache.cores)
            return None
        
        access_dict[cache_bottom.name] = 0
        access_dict[cache_middle.name] = 0
        access_dict[cache_top.name]    = 0
        for tileid in tileids:
            cache_bottom_hit = cache_bottom.access(tileid)
            cache_middle_hit = cache_middle.access(tileid)
            cache_top_hit    = cache_top.access(tileid)
            
            if cache_top_hit == False: # Memory miss
                if cache_middle_hit == True:
                    print("Cache error!")
                    return None
                if cache_bottom_hit == True:
                    print("Cache error!")
                    return None
                
                removed_element = cache_top.insert(tileid)
                if(removed_element is not None):
                    cache_top.remove(removed_element)
                
                removed_element = cache_middle.insert(tileid)
                if(removed_element is not None):
                    cache_middle.remove(removed_element)
                    
                removed_element = cache_bottom.insert(tileid)
            
            elif cache_middle_hit == False: # L3 miss
                if cache_bottom_hit == True:
                    print("Cache error!")
                    return None
                
                removed_element = cache_middle.insert(tileid)
                if(removed_element is not None):
                    cache_middle.remove(removed_element)
                    
                removed_element = cache_bottom.insert(tileid)
            
            elif cache_bottom_hit == False:
                # Nothing to do here other than insert the element
                removed_element = cache_bottom.insert(tileid)
                
            else:
                pass # all accesses succeeded.
        
            access_dict[cache_bottom.name] += int(cache_bottom_hit)
            access_dict[cache_middle.name] += int(cache_middle_hit)
            access_dict[cache_top.name]    += int(cache_top_hit)
        return access_dict
        
# returns: a "Memory" class
def setup_caches(xmlinfo, tilesize, print_cache = False):
    # TODO: be smarter with xmlinfo
    xmldict = xmltodict.parse(xmlinfo)
    tile_bytes = size_of_tile(tilesize) # TODO: remove assumption of double precision
    # Currently assumes structure of the xmldict.
    # TODO: it would be cool to fix that...
    machine = xmldict['topology']['object']['object']
    main_memory = Cache("sim_MM", 10e12, 10e12) # memory has practically infinite size
    for NUMA_node_idx in range(len(machine)):
        machine_L3_cache = machine[NUMA_node_idx]['object'][1]
        L3_size = int(machine_L3_cache['@cache_size'])
        L3_cache = Cache("sim_L3", L3_size, L3_size//tile_bytes)
        
        for L2_idx in range(len(machine_L3_cache['object'])):
            machine_L2_cache = machine_L3_cache['object'][L2_idx]
            L2_size = int(machine_L2_cache['@cache_size'])
            L2_cache = Cache("sim_L2", L2_size, L2_size//tile_bytes)
            L2_cache.cores.append(int(machine_L2_cache['object']['object']['object'][0]['@os_index']))
            L2_cache.cores.append(int(machine_L2_cache['object']['object']['object'][1]['@os_index']))
            # print(f"DEBUG: appending an L2 cache with cores {L2_cache.cores}")
            L3_cache.subcaches.append(L2_cache)
        
        for child in L3_cache.subcaches:
            for core in child.cores:
                L3_cache.cores.append(core)
        main_memory.subcaches.append(L3_cache)
    for child in main_memory.subcaches:
        for core in child.cores:
            main_memory.cores.append(core)
            
    if(print_cache):
        print("Cache heirarchy")
        print(f"main memory - size: {main_memory.size}\t tiles: {main_memory.max_residents}")
        for middle_cache in main_memory.subcaches:
            print(f"    {middle_cache.name} - size: {middle_cache.size}\t tiles: {middle_cache.max_residents}")
            for bottom_cache in middle_cache.subcaches:
                print(f"        {bottom_cache.name} - size: {bottom_cache.size}\t tiles: {bottom_cache.max_residents}")
            
    return Memory(main_memory)

################################################################################################
# trace function
################################################################################################
# Some globals are needed for the below function:
# A_status, B_status, C_status, A_expected, B_expected, C_expected
# These lists of lists represent the progress of the job on each tile
# Such lists are modified both by animate_trace and a function within it,
# which is necessary to implement in order to achieve the format that 
# is required by FuncAnimation.
# The `expected` variables are so that calculating progress as a percentage
# is feasible.
# E_status is the status of the 'extra' matrix in the 2x2 representation. Currently
# it is being used for core swaps.
# The last globals are
# `tasks_at_frame`
# This is an array of the number of tasks which have executed since the previous
# frame.
# `core_work_tiles`
# A list of the resource's cores alongside the last tile of C that core previously
# performed work on. If a new tile of C is worked on, but the `core_work_tiles`
# reveals that the the core working on this tile of C previously worked on a
# different tile, then we know a cache miss was guaranteed.

def animate_trace(trace, 
                  task_type,
                  order_func = None,
                  which_animate = "tasks",
                  title = "unknown desc (pass something as 'title')",
                  num_frames = 50,         # Number of frames to use in the animation
                  enforce_interval = None, # Alternatively, determines a timestep per frame (in seconds)
                  fps = 13,
                  fill = "relative", # relative for all will be yellow, absolute for some will be fully yellow
                  M = None, # Must be provided
                  N = None, # Must be provided
                  K = None, # Must be provided (even if the geometry of the matrix doesn't make sense for it)
                  tilesize = None, # Must be provided
                  bigtilesize = None # Optional
                 ):
    
    global A_status
    global B_status
    global C_status
    global E_status
    global tasks_at_frame
    global core_work_tiles
    global core_migrations
    global migrations_dict
    global cache_sim_dict
    global core_hits
    global sim_memory
    global A_expected # Currently unimplemented
    global B_expected
    global C_expected
    start_time = time.perf_counter()
    print("Beginning animation of data '%s' method '%s'" % (title, which_animate))
    
    # Begin checks
    error = check_parameters_basic(M, N, K, tilesize, fill, which_animate)
    if error == True:
        return
        
    
    # Guess the trace type
    trace_type = guess_trace_type(trace)
    print("After observing your trace, I am guessing it is from the %s interface" % trace_type)
    
    running_system = guess_running_system(trace)
    print("I think this trace is for a job that was running on", running_system)
    
    error = check_for_order_function(trace_type, order_func)
    if error == True:
        return
    
    
    error = check_parameter_compatibility(task_type, trace_type, which_animate, M, N, K)
    if error == True:
        return
    
    
    # Begin estimation and preprocessing
    name_to_task_num = get_work_tasks_indices(trace, task_type, running_system)
    if name_to_task_num is None:
        return
    work_tasks_indices = work_tasks_indices_from_names_dict(name_to_task_num)
    if work_tasks_indices == []:
        return
    
    # Now isolate just the work tasks
    # This is the step that seems to take forever.
    tta = time.perf_counter()
    work_tasks = trace.events.type.isin(work_tasks_indices)
    if trace_type == "dtd":
        # even though the plots produced plot by end time, the task ids are monotone
        # with task insertion (begin) time, so this is what dtd requires sorting by
        # orderdf = pd.DataFrame(trace.events[work_tasks].sort_values("begin"))
        orderdf = pd.DataFrame(trace.events[work_tasks])
    elif trace_type == "ptg":
        orderdf = pd.DataFrame(trace.events[work_tasks])
    ttb = time.perf_counter()
    print("time to make df: %f seconds" % (ttb-tta))
    
    if task_type == "potrf":
        potrf_uplo = get_potrf_uplo(running_system, orderdf, name_to_task_num)
        if potrf_uplo is None:
            return
        print("I think this is potrf is %s triangular" % potrf_uplo)
        
    first_begin = orderdf["begin"].min()
    last_end =  orderdf["end"].max()
        
    Ntiles_m = math.ceil(M/tilesize)
    Ntiles_n = math.ceil(N/tilesize)
    Ntiles_k = math.ceil(K/tilesize)
    
    
    time_per_frame = get_time_per_frame(first_begin, last_end, num_frames, enforce_interval)
    if time_per_frame is None:
        return
    print("process runtime per frame: %f seconds" % (time_per_frame))
    
    
    estimated_compile_time = get_estimated_video_time(which_animate, running_system,
                                                      task_type, num_frames,
                                                      Ntiles_m, Ntiles_n)
    print("estimated video compile time (assuming a lightweight commercial processor): %f seconds" %
          estimated_compile_time)
    
    
    expected_tasks = get_expected_tasks(task_type, running_system,
                                        Ntiles_m, Ntiles_n, Ntiles_k,
                                        tilesize, bigtilesize)
    if expected_tasks is None:
        return
    
    
    # This ID array is only needed for dtd traces.
    # (Because the trace for DTD gemm does not supply us with
    # m, n, and k, we must remember the order we insterted the tasks
    # and re-extract that data as such:
    if trace_type == "dtd":
        (id_orders, ideal_order, indices_arr) = get_dtd_helpers(orderdf, order_func, Ntiles_m, Ntiles_n, Ntiles_k)
        if id_orders is None or ideal_order is None or indices_arr is None:
            return
    
    
    # End estimation and preprocessing
    # Prepare the arrays used for the animation
    init_global_arrays(trace, Ntiles_m, Ntiles_n, Ntiles_k, which_animate)
    
    # Prepare the figure that will be displayed
    figsize_dims = get_figsize_dimensions(Ntiles_m, Ntiles_n)
    fig, ((axE, axB), (axA, axC)) = init_plt_figure(which_animate, figsize_dims)
    
    
    sim_memory = setup_caches(trace["information"]["HWLOC-XML"], tilesize)
    
    # Enter the animation functions
    # plots: 'c' for just plotting matrix c, 'abc' for all three
    # mode: 'progress' for keeping the tiles colored, 'tasks' to reset them and see what was active and when.
    
    # FuncAnimate works best with an init function, even if it doesn't do much or the init is done elsewhere.
    def animate_init_common(plots):
        global A_status
        global B_status
        global C_status
        global E_status
        global tasks_at_frame
        global core_work_tiles
        global core_migrations
        global migrations_dict
        global cache_sim_dict
        global core_hits
        global sim_memory
        global A_expected
        global B_expected
        global C_expected
        tasks_at_frame = []
        C_status = C_status * 0
        if(plots == "abc"):
            A_status = A_status * 0
            B_status = B_status * 0
        
        # Check to see if we have as many tasks as we should expect
        if(expected_tasks != len(orderdf)):
            print("warning: it seems like your trace has %d tasks, but I expected %d "
                     % (len(orderdf), expected_tasks)
                     + "based on the arguments you provided for N, M, K, and tilesize.")
            
        core_migrations = core_hits = 0
            
        if task_type == "gemm": # it is irrelevant whether fill is relative or absolute here.
            migrations_dict = {}
            cache_sim_dict ={}
            C_expected += Ntiles_k
            if plots == "abc":
                A_expected += Ntiles_n
                B_expected += Ntiles_m
        elif task_type == "potrf":
            if fill == "relative":
                for i in range(len(C_expected)):
                    for j in range(len(C_expected[0])):
                        if j <= i: # For lower triangular
                            C_expected[i,j] = j+1
                        elif j >= i: # For upper triangular
                            C_expected[i,j] = i+1
            elif fill == "absolute":
                C_expected += len(C_expected[0])
        else:
            print("Error: unknown task type")
        return
    
    def animate_init_abc():
        return animate_init_common("abc")
    
    def animate_init_c():
        return animate_init_common("c")
    
    # for the actual animation
    def animate_gemm_common(frame, trace_type, plots, mode):
        global A_status
        global B_status
        global C_status
        global E_status
        global tasks_at_frame
        global core_work_tiles
        global core_migrations
        global migrations_dict
        global cache_sim_dict
        global core_hits
        global sim_memory
        global A_expected
        global B_expected
        global C_expected
        # time_point_curr and time_point_prev in units of nanoseconds
        time_point_curr = ((frame+0)*(last_end - first_begin))//num_frames + first_begin
        time_point_prev = ((frame-1)*(last_end - first_begin))//num_frames + first_begin
        if(frame == 0): # One empty frame at start
            time_point_prev = time_point_cur = first_begin
        if(frame >  num_frames): # This construction to allow dead frames at the end
            time_point_prev = time_point_curr = last_end
        # progress bar
        bar_length = 70
        num_bars = math.ceil((bar_length*frame/num_frames))
        print("\r[", end="")
        for i in range(bar_length):
            if(num_bars > i):
                print("#", end="")
            else:
                print("-", end="")
        print("]",end="")
            
        if(mode == "tasks"):
            C_status = C_status * 0 # Always zero when doing task timing
            if(plots == "abc"):
                A_status = A_status * 0 # Always zero when doing task timing
                B_status = B_status * 0 # Always zero when doing task timing 
                
        tasks_before = orderdf.loc[orderdf["end"] <= time_point_curr]
        tasks_during = tasks_before.loc[(tasks_before["end"] > time_point_prev)]
            # aside: you might think this construction with > time_point_prev wouldn't include the first task,
            #        but this is fine! The only assumption made is that the first task does not end the same nanoseond
            #        that it begins. That sounds like a safe assumption to me.
        
        tasks_at_frame.append(len(tasks_during))
        
        for index, task in tasks_during.iterrows():
            core = task["stream_id"]
            if trace_type == "dtd":
                tid = task["id"]
                tid_normed = indices_arr[np.where(id_orders == tid)][0]
                # element = ideal_order[indices_arr[tid_normed]]
                element = ideal_order[tid_normed]
                m = element[0]
                n = element[1]
                k = element[2]
            elif trace_type == "ptg":
                m = task["m"]
                n = task["n"]
                k = task["k"]
                
            if(plots == "c"):
                if mode == "swaps":
                    if(core_work_tiles[core][0] != m or
                       core_work_tiles[core][1] != n):
                        core_work_tiles[core] = [m, n, k] 
                        core_migrations += 1
                        if(C_status[m, n] == 0):
                            migrations_dict[task["id"]] = "first access"
                        else:
                            migrations_dict[task["id"]] = "migrated"
                        C_status[m, n] += 1 / C_expected[m, n]
                    else:
                        core_hits += 1
                        migrations_dict[task["id"]] = "reused"
                elif mode in ["tasks", "progress"]:
                    C_status[m, n] += 1 / C_expected[m, n]
                else:
                    print("error: unknown mode", mode)
            elif(plots == "abc"):
                if (mode == "progress"):
                    if(core_work_tiles[core][0] != m or
                       core_work_tiles[core][1] != n):
                        showing_this_to_the_crew = False
                        if(showing_this_to_the_crew):
                            prio = task["priority"]
                            if(k == 0):
                                pass
                            else:
                                print(f"core miss: core {core} last worked on {core_work_tiles[core]}, ", end="")
                                print(f"but now working on tile {[m,n,k]} (priority {prio}).")
                                print(f"methane core calculation: {(m%6)+6*(n%6)} saturn: {(m%5)+5*(n%4)}")
                        
                        
                        core_migrations += 1
                        if (C_status[m, n] == 0):
                            migrations_dict[task["id"]] = "first access"
                        else:
                            migrations_dict[task["id"]] = "migrated"
                            
                        core_work_tiles[core] = [m, n, k] 
                        E_status[m, n] += 1 / C_expected[m, n]
                    else:
                        core_hits += 1
                        migrations_dict[task["id"]] = "reused"
                A_status[m, k] += 1 / A_expected[m, k]
                B_status[k, n] += 1 / B_expected[k, n]
                C_status[m, n] += 1 / C_expected[m, n]
                
            # always run cache simulation, regardless of whether the migrations check is done
            # TODO: determine which cache a task applies to
            cache_sim_dict[task["id"]] = sim_memory.access(core, m, n, k)
                
        if(plots == "c"):
            fig.suptitle("Problem at time t=%4.6fms (%s)" % (time_point_curr/10**6, title))
            if(mode == "swaps"):
                axC.set_title("Matrix C (shaded each time a cache miss guaranteed)")
            else:
                axC.set_title("Matrix C")
            axC.pcolormesh(C_status, vmin = 0, vmax = 1)
        elif(plots == "abc"):
            fig.suptitle("Problem at time t=%4.6f s (%s)" % (time_point_curr/10**9, title))
            axA.set_title("Matrix A")
            axA.pcolormesh(A_status, vmin = 0, vmax = 1)
            axB.set_title("Matrix B")
            axB.pcolormesh(B_status, vmin = 0, vmax = 1)
            axC.set_title("Matrix C")
            axC.pcolormesh(C_status, vmin = 0, vmax = 1)
            if (mode == "progress"):
                axE.set_title("Matrix C (core migration)")
                axE.pcolormesh(E_status, vmin = 0, vmax = 1)
                
        return
    
    def animate_gemm_dtd_order_with_time(frame):
        return animate_gemm_common(frame, "dtd", "c", "tasks")
    
    def animate_gemm_dtd_order_with_time_all(frame):
        return animate_gemm_common(frame, "dtd", "abc", "tasks")
    
    def animate_gemm_dtd_order_core_swaps(frame):
        return animate_gemm_common(frame, "dtd", "c", "swaps")
    
    def animate_gemm_dtd_order_progress(frame):
        return animate_gemm_common(frame, "dtd", "c", "progress")
        
    def animate_gemm_dtd_order_progress_all(frame):
        return animate_gemm_common(frame, "dtd", "abc", "progress")
    
    def animate_gemm_ptg_order_with_time(frame):
        return animate_gemm_common(frame, "ptg", "c", "tasks")
    
    def animate_gemm_ptg_order_with_time_all(frame):
        return animate_gemm_common(frame, "ptg", "abc", "tasks")
    
    def animate_gemm_ptg_order_core_swaps(frame):
        return animate_gemm_common(frame, "ptg", "c", "swaps")
    
    def animate_gemm_ptg_order_progress(frame):
        return animate_gemm_common(frame, "ptg", "c", "progress")
        
    def animate_gemm_ptg_order_progress_all(frame):
        return animate_gemm_common(frame, "ptg", "abc", "progress")
    
    def animate_potrf_common(frame, mode):
        global C_status
        global tasks_at_frame
        global C_expected
        global large_tile # TODO: revisit this like-to-be-wrong heuristic
        # time_point_curr and time_point_prev in units of nanoseconds
        time_point_curr = ((frame+0)*(last_end - first_begin))//num_frames + first_begin
        time_point_prev = ((frame-1)*(last_end - first_begin))//num_frames + first_begin
        if(frame == 0): # One empty frame at start
            time_point_prev = time_point_cur = first_begin
        if(frame >  num_frames): # This construction to allow dead frames at the end
            time_point_prev = time_point_curr = last_end
            
        if(mode == "tasks"):
            C_status = C_status * 0 # Always zero when doing task timing
            
        tasks_before = orderdf.loc[orderdf["end"] <= time_point_curr]
        tasks_during = tasks_before.loc[(tasks_before["end"] > time_point_prev)]
            # aside: you might think this construction with > time_point_prev wouldn't include the first task,
            #        but this is fine! The only assumption made is that the first task does not end the same nanoseond
            #        that it begins. That sounds like a safe assumption to me.
        
        tasks_at_frame.append(len(tasks_during))
        
        for index, task in tasks_during.iterrows():
            m = task["m"]
            n = task["n"]
            k = task["k"]
            # print("type:", task["type"], "m,n,k:", m,n,k)
            # uplo of the matrix order the m, n, and k differently for tasks
            if running_system == "dplasma": # dplasma: upper triangular by default
                if task["type"] == name_to_task_num["potrf"]:
                    target_row = k
                    target_col = k
                elif task["type"] == name_to_task_num["trsm"]:
                    if potrf_uplo == "upper":
                        target_row = k
                        target_col = n
                    elif potrf_uplo == "lower":
                        target_row = m
                        target_col = k
                elif task["type"] == name_to_task_num["syrk"]:
                    if potrf_uplo == "upper":
                        target_row = n
                        target_col = n
                    elif potrf_uplo == "lower":
                        target_row = m
                        target_col = m
                elif task["type"] == name_to_task_num["gemm"]:
                    target_row = m
                    target_col = n
                else:
                    print("error: unexpected task of type", task["type"])
                    print(task)
                if target_row is None or target_col is None:
                    for key in name_to_task_num:
                        if task["type"] == name_to_task_num[key]:
                            offending_task = key
                            break
                    print(f"a parameter was none that shouldn't have been ({offending_task})")
                C_status[target_row, target_col] += 1 / C_expected[target_row, target_col]
            elif running_system == "hicma": # hicma: lower triangular by default
                stride = bigtilesize // tilesize
                if task["type"] == name_to_task_num["large-potrf"]:
                    target_row = k
                    target_col = k
                    large_task = True
                    large_tile = k
                elif task["type"] == name_to_task_num["large-trsm"]:
                    if potrf_uplo == "upper":
                        target_row = k
                        target_col = n
                    elif potrf_uplo == "lower":
                        target_row = m
                        target_col = k
                    large_task = True
                elif task["type"] == name_to_task_num["large-syrk"]:
                    if potrf_uplo == "upper":
                        target_row = n
                        target_col = n
                    elif potrf_uplo == "lower":
                        target_row = m
                        target_col = m
                    large_task = True
                elif task["type"] == name_to_task_num["large-gemm"]:
                    target_row = m
                    target_col = n
                    large_task = True
                elif task["type"] == name_to_task_num["small-potrf"]:
                    target_row = k
                    target_col = k
                    large_task = False
                elif task["type"] == name_to_task_num["small-trsm"]:
                    if potrf_uplo == "upper":
                        target_row = k
                        target_col = n
                    elif potrf_uplo == "lower":
                        target_row = m
                        target_col = k
                    large_task = False
                elif task["type"] == name_to_task_num["small-syrk"]:
                    if potrf_uplo == "upper":
                        target_row = n
                        target_col = n
                    elif potrf_uplo == "lower":
                        target_row = m
                        target_col = m
                    large_task = False
                elif task["type"] == name_to_task_num["small-gemm"]:
                    target_row = m
                    target_col = n
                    large_task = False
                else:
                    print("error: unexpected task of type %d" % task["type"])
                    print(task)
                if target_row is None or target_col is None:
                    for key in name_to_task_num:
                        if task["type"] == name_to_task_num[key]:
                            offending_task = key
                            break
                    print(f"a parameter was none that shouldn't have been ({offending_task})")
                    
                if large_task == True:
                    for i in range(target_row*stride, (target_row+1)*stride):
                        for j in range(target_col*stride, (target_col+1)*stride):
                            if(task["type"] == name_to_task_num["large-potrf"]):
                                # This is a recursive task: no updates.
                                # The updates are spawned by its children
                                continue
                            elif(task["type"] == name_to_task_num["large-syrk"]):
                                if(j > i):
                                    continue # No suprise
                                C_status[i, j] += stride / C_expected[i, j]
                            elif(task["type"] == name_to_task_num["large-trsm"]):
                                C_status[i, j] += ((j % stride)+1) / C_expected[i, j]
                            elif(task["type"] == name_to_task_num["large-gemm"]):
                                C_status[i, j] += stride / C_expected[i, j]
                            else:
                                print("error: unexpected task of type", task["type"])
                else:
                    target_row += stride * large_tile
                    target_col += stride * large_tile
                    C_status[target_row, target_col] += 1 / C_expected[target_row, target_col]
            else:
                print("Error: unknown running system")
                
        fig.suptitle("Problem at time t=%4.6f s (%s)" % (time_point_curr/10**9, title))
        axC.set_title("POTRF Target")
        axC.pcolormesh(C_status, vmin = 0, vmax = 1)
        return
    
    def animate_potrf_tasks(frame):
        return animate_potrf_common(frame, "tasks")
        
    def animate_potrf_progress(frame):
        return animate_potrf_common(frame, "progress")
    
    if(task_type == "gemm"):
        if(trace_type == "dtd"):
            if(which_animate == "tasks"):
                animation_func = animate_gemm_dtd_order_with_time
                animation_init = animate_init_c
            elif(which_animate == "progress"):
                animation_func = animate_gemm_dtd_order_progress
                animation_init = animate_init_c
            elif(which_animate == "coreswaps"):
                animation_func = animate_gemm_dtd_order_core_swaps
                animation_init = animate_init_c
            elif(which_animate == "abctasks"):
                animation_func = animate_gemm_dtd_order_with_time_all
                animation_init = animate_init_abc
            elif(which_animate == "abcprogress"):
                animation_func = animate_gemm_dtd_order_progress_all
                animation_init = animate_init_abc
            else:
                print("error: no animation functions")
        elif(trace_type == "ptg"):
            if(which_animate == "tasks"):
                animation_func = animate_gemm_ptg_order_with_time
                animation_init = animate_init_c
            elif(which_animate == "progress"):
                animation_func = animate_gemm_ptg_order_progress
                animation_init = animate_init_c
            elif(which_animate == "coreswaps"):
                animation_func = animate_gemm_ptg_order_core_swaps
                animation_init = animate_init_c
            elif(which_animate == "abctasks"):
                animation_func = animate_gemm_ptg_order_with_time_all
                animation_init = animate_init_abc
            elif(which_animate == "abcprogress"):
                animation_func = animate_gemm_ptg_order_progress_all
                animation_init = animate_init_abc
            else:
                print("error: no animation functions")
        else:
            print("error: unrecognized trace type")
    elif (task_type == "potrf"):
        animation_init = animate_init_c
        if(which_animate == "tasks"):
            animation_func = animate_potrf_tasks
        elif(which_animate == "progress"):
            animation_func = animate_potrf_progress
        
    mid_time = time.perf_counter()
    # Supply a half second of stillness at the end
    padding_frames = max(fps // 2, 2)
    animation_result = FuncAnimation(fig,animation_func, init_func = animation_init,
                                     frames=(num_frames+padding_frames),interval=1000//fps)
    video = animation_result.to_html5_video()
    html = display.HTML(video)
    display.display(html)
    plt.close()
    

    end_time = time.perf_counter()
    ## Printing
    
    orderdf = orderdf.sort_values("begin")
    
    mgd2 = {"id": [], "migrated": []}
    csd2 = {"id": [], "sim_L2_hit": [], "sim_L3_hit": [], "sim_MM_hit": []}
    for key in migrations_dict:
        mgd2["id"].append(key)
        mgd2["migrated"].append(migrations_dict[key])
    for key in cache_sim_dict:
        csd2["id"].append(key)
        for cache_type in cache_sim_dict[key]:
            csd2[cache_type+"_hit"].append(cache_sim_dict[key][cache_type])
    migrations_df = pd.DataFrame.from_dict(mgd2)
    sim_cache_hit_df = pd.DataFrame.from_dict(csd2)
    # print(orderdf)
    # print(migrations_df)
    orderdf["new_idx"] = np.arange(len(orderdf))
    # duration: convert from ns to ms
    orderdf["duration"] = (orderdf["end"]-orderdf["begin"]) / 1000000
    orderdf = orderdf.join(migrations_df.set_index("id"), on="id")
    orderdf = orderdf.join(sim_cache_hit_df.set_index("id"), on="id")
    # print(orderdf)
    # print(orderdf["migrated"])
    # TODO: left off here.
    if(task_type == "gemm" and (which_animate in ["coreswaps", "abcprogress"])):
       do_core_migration_plots = True
    else:
       do_core_migration_plots = False
    migrated_df = orderdf[orderdf.loc[:,"migrated"] == "migrated"]
    core_hit_df = orderdf[orderdf.loc[:,"migrated"] == "reused"]
    f_access_df = orderdf[orderdf.loc[:,"migrated"] == "first access"]
    sim_L2_3hit_df = orderdf[orderdf.loc[:,"sim_L2_hit"] == 3]
    sim_L2_2hit_df = orderdf[orderdf.loc[:,"sim_L2_hit"] == 2]
    sim_L2_1hit_df = orderdf[orderdf.loc[:,"sim_L2_hit"] == 1]
    sim_L2_miss_df = orderdf[orderdf.loc[:,"sim_L2_hit"] == 0]
    sim_L3_3hit_df = orderdf[orderdf.loc[:,"sim_L3_hit"] == 3]
    sim_L3_2hit_df = orderdf[orderdf.loc[:,"sim_L3_hit"] == 2]
    sim_L3_1hit_df = orderdf[orderdf.loc[:,"sim_L3_hit"] == 1]
    sim_L3_miss_df = orderdf[orderdf.loc[:,"sim_L3_hit"] == 0]
    sim_MM_3hit_df = orderdf[orderdf.loc[:,"sim_MM_hit"] == 3]
    sim_MM_2hit_df = orderdf[orderdf.loc[:,"sim_MM_hit"] == 2]
    sim_MM_1hit_df = orderdf[orderdf.loc[:,"sim_MM_hit"] == 1]
    sim_MM_miss_df = orderdf[orderdf.loc[:,"sim_MM_hit"] == 0]
    # print(orderdf[orderdf.loc[:,"migrated"] == True])
    # tasks_migrations = orderdf["migrated"].to_numpy()
    # print(tasks_migrations)
    # for thing in tasks_migrations:
        # print(thing)
    tasks_times = (orderdf["duration"]).to_numpy()
    migrated_tasks_times = (migrated_df["duration"]).to_numpy()
    core_hit_tasks_times = (core_hit_df["duration"]).to_numpy()
    f_access_tasks_times = (f_access_df["duration"]).to_numpy()
    sim_L2_1hit_tasks_times = (sim_L2_1hit_df["duration"]).to_numpy()
    sim_L2_2hit_tasks_times = (sim_L2_2hit_df["duration"]).to_numpy()
    sim_L2_3hit_tasks_times = (sim_L2_3hit_df["duration"]).to_numpy()
    sim_L2_miss_tasks_times = (sim_L2_miss_df["duration"]).to_numpy()
    sim_L3_1hit_tasks_times = (sim_L3_1hit_df["duration"]).to_numpy()
    sim_L3_2hit_tasks_times = (sim_L3_2hit_df["duration"]).to_numpy()
    sim_L3_3hit_tasks_times = (sim_L3_3hit_df["duration"]).to_numpy()
    sim_L3_miss_tasks_times = (sim_L3_miss_df["duration"]).to_numpy()
    sim_MM_1hit_tasks_times = (sim_MM_1hit_df["duration"]).to_numpy()
    sim_MM_2hit_tasks_times = (sim_MM_2hit_df["duration"]).to_numpy()
    sim_MM_3hit_tasks_times = (sim_MM_3hit_df["duration"]).to_numpy()
    sim_MM_miss_tasks_times = (sim_MM_miss_df["duration"]).to_numpy()
    tasks_execution_mean = tasks_times.mean()
    tasks_execution_min  = tasks_times.min()
    tasks_execution_sdev = tasks_times.std()
    tasks_runtime = (last_end - first_begin) / 1000000 # in ms
    core_utilization = (tasks_execution_mean * len(tasks_times)) / tasks_runtime
    eff_utilization = (tasks_execution_min * len(tasks_times)) / tasks_runtime
    num_cores = trace.information["nb_cores"]
    print("Data titled '%s'" % title)
    print("M=%d,\tN=%d,\tK=%d,\ttilesize=%d" % (M,N,K,tilesize))
    if(task_type == "gemm"):
        # *1000 : FLOPS/ms -> FLOPS/s
        # /1e9 : FLOPS/s -> GFLOPS/s
        print("GEMM performance: %f GFLOPS" % ((M*N*K*2)/tasks_runtime*1000/1e9))
    print()
    I_want_to_see_task_times = False
    if (I_want_to_see_task_times == True):
        print("average task execution time (ms): %f" % tasks_execution_mean)
        print("migrated: %f\treuse: %f\tfirst access: %f" %
                (migrated_tasks_times.mean(), core_hit_tasks_times.mean(), f_access_tasks_times.mean()))
        print("task execution time standard deviation (ms): %f" % tasks_execution_sdev)
        print("migrated: %f\treuse: %f\tfirst access: %f" %
                (migrated_tasks_times.std(), core_hit_tasks_times.std(), f_access_tasks_times.std()))
    print("core utilization: %f over %d cores (%4.2f%s)" % (core_utilization, num_cores, core_utilization/num_cores*100, "%"))
    print("    (so %4.2f%s spent in scheduler)" % (((1-core_utilization/num_cores)*100), "%"))
    print("efficiency utilization: %4.2f%s (based on the amount of time a task *should* take)"
           % (eff_utilization/num_cores*100, "%"))
    if(task_type == "gemm" and (which_animate in ["coreswaps", "abcprogress"]) and
       (core_migrations - len(C_status)*len(C_status[0]) + core_hits != 0)):
        core_migrations -= len(C_status)*len(C_status[0])
        print("GEMM core migration percentage: %2.3f%s" % ((core_migrations/(core_hits+core_migrations))*100, "%"))
    print(f"core migrations + first touches: {len(migrated_tasks_times) + len(f_access_tasks_times)}")
    print(f"simulated L2 misses: {3*len(sim_L2_miss_df)+2*len(sim_L2_1hit_df)+1*len(sim_L2_2hit_df)}")
    print(f"simulated L3 misses: {3*len(sim_L3_miss_df)+2*len(sim_L3_1hit_df)+1*len(sim_L3_2hit_df)}")
    print(f"simulated MM misses: {3*len(sim_MM_miss_df)+2*len(sim_MM_1hit_df)+1*len(sim_MM_2hit_df)}")
    
    # Prune some data for violin, pruned plots
    prune_threshold = tasks_execution_mean*5
    pruned_orderdf = orderdf[orderdf.loc[:, "duration"] < prune_threshold]
    pruned_migrated_df = pruned_orderdf[pruned_orderdf.loc[:,"migrated"] == "migrated"]
    pruned_core_hit_df = pruned_orderdf[pruned_orderdf.loc[:,"migrated"] == "reused"]
    pruned_f_access_df = pruned_orderdf[pruned_orderdf.loc[:,"migrated"] == "first access"]
    pruned_sim_L2_3hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_L2_hit"] == 3]
    pruned_sim_L2_2hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_L2_hit"] == 2]
    pruned_sim_L2_1hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_L2_hit"] == 1]
    pruned_sim_L2_miss_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_L2_hit"] == 0]
    pruned_sim_L3_3hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_L3_hit"] == 3]
    pruned_sim_L3_2hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_L3_hit"] == 2]
    pruned_sim_L3_1hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_L3_hit"] == 1]
    pruned_sim_L3_miss_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_L3_hit"] == 0]
    pruned_sim_MM_3hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_MM_hit"] == 3]
    pruned_sim_MM_2hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_MM_hit"] == 2]
    pruned_sim_MM_1hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_MM_hit"] == 1]
    pruned_sim_MM_miss_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_MM_hit"] == 0]
    
    ##### Again for pruned data
    pruned_migrated_tasks_times = (pruned_migrated_df["duration"]).to_numpy()
    pruned_core_hit_tasks_times = (pruned_core_hit_df["duration"]).to_numpy()
    pruned_f_access_tasks_times = (pruned_f_access_df["duration"]).to_numpy()
    pruned_sim_L2_3hit_tasks_times = (pruned_sim_L2_3hit_df["duration"]).to_numpy()
    pruned_sim_L2_2hit_tasks_times = (pruned_sim_L2_2hit_df["duration"]).to_numpy()
    pruned_sim_L2_1hit_tasks_times = (pruned_sim_L2_1hit_df["duration"]).to_numpy()
    pruned_sim_L2_miss_tasks_times = (pruned_sim_L2_miss_df["duration"]).to_numpy()
    pruned_sim_L3_3hit_tasks_times = (pruned_sim_L3_3hit_df["duration"]).to_numpy()
    pruned_sim_L3_2hit_tasks_times = (pruned_sim_L3_2hit_df["duration"]).to_numpy()
    pruned_sim_L3_1hit_tasks_times = (pruned_sim_L3_1hit_df["duration"]).to_numpy()
    pruned_sim_L3_miss_tasks_times = (pruned_sim_L3_miss_df["duration"]).to_numpy()
    pruned_sim_MM_3hit_tasks_times = (pruned_sim_MM_3hit_df["duration"]).to_numpy()
    pruned_sim_MM_2hit_tasks_times = (pruned_sim_MM_2hit_df["duration"]).to_numpy()
    pruned_sim_MM_1hit_tasks_times = (pruned_sim_MM_1hit_df["duration"]).to_numpy()
    pruned_sim_MM_miss_tasks_times = (pruned_sim_MM_miss_df["duration"]).to_numpy()
    migrated_tt_mean = migrated_tasks_times.mean()
    core_hit_tt_mean = core_hit_tasks_times.mean()
    f_access_tt_mean = f_access_tasks_times.mean()
    sL2_3hit_tt_mean = sim_L2_3hit_tasks_times.mean()
    sL2_2hit_tt_mean = sim_L2_2hit_tasks_times.mean()
    sL2_1hit_tt_mean = sim_L2_1hit_tasks_times.mean()
    sL2_miss_tt_mean = sim_L2_miss_tasks_times.mean()
    sL3_3hit_tt_mean = sim_L3_3hit_tasks_times.mean()
    sL3_2hit_tt_mean = sim_L3_2hit_tasks_times.mean()
    sL3_1hit_tt_mean = sim_L3_1hit_tasks_times.mean()
    sL3_miss_tt_mean = sim_L3_miss_tasks_times.mean()
    sMM_3hit_tt_mean = sim_MM_3hit_tasks_times.mean()
    sMM_2hit_tt_mean = sim_MM_2hit_tasks_times.mean()
    sMM_1hit_tt_mean = sim_MM_1hit_tasks_times.mean()
    sMM_miss_tt_mean = sim_MM_miss_tasks_times.mean()
    # sMM_miss_tt_std  = sim_MM_miss_tasks_times.std()
    migrated_ptt_mean = pruned_migrated_tasks_times.mean()
    core_hit_ptt_mean = pruned_core_hit_tasks_times.mean()
    f_access_ptt_mean = pruned_f_access_tasks_times.mean()
    sL2_3hit_ptt_mean = pruned_sim_L2_3hit_tasks_times.mean()
    sL2_2hit_ptt_mean = pruned_sim_L2_2hit_tasks_times.mean()
    sL2_1hit_ptt_mean = pruned_sim_L2_1hit_tasks_times.mean()
    sL2_miss_ptt_mean = pruned_sim_L2_miss_tasks_times.mean()
    sL3_3hit_ptt_mean = pruned_sim_L3_3hit_tasks_times.mean()
    sL3_2hit_ptt_mean = pruned_sim_L3_2hit_tasks_times.mean()
    sL3_1hit_ptt_mean = pruned_sim_L3_1hit_tasks_times.mean()
    sL3_miss_ptt_mean = pruned_sim_L3_miss_tasks_times.mean()
    sMM_3hit_ptt_mean = pruned_sim_MM_3hit_tasks_times.mean()
    sMM_2hit_ptt_mean = pruned_sim_MM_2hit_tasks_times.mean()
    sMM_1hit_ptt_mean = pruned_sim_MM_1hit_tasks_times.mean()
    sMM_miss_ptt_mean = pruned_sim_MM_miss_tasks_times.mean()
    # sMM_miss_ptt_std  = pruned_sim_MM_miss_tasks_times.std()
    if (I_want_to_see_task_times == True):
        print("average task execution time (ms): %f" % tasks_execution_mean)
        print("migrated: %f\treuse: %f\tfirst access: %f" % (migrated_ptt_mean, core_hit_ptt_mean, f_access_ptt_mean))
        print("task execution time standard deviation (ms): %f" % tasks_execution_sdev)
        print("migrated: %f\treuse: %f\tfirst access: %f" % (migrated_ptt_std, core_hit_ptt_std, f_access_ptt_std))
    
    
    print("pruned graphs have removed %4.2f%s of the data (anything above %4.5fms)" %
           ((1-len(pruned_orderdf)/len(orderdf))*100, "%", prune_threshold))
    
    print()
    print("execution time to generate graphs: %f seconds" % (end_time-mid_time))
    print("execution time to preprocess data: %f seconds" % (mid_time-start_time))
    print("execution time total: %f seconds" % (end_time-start_time))
    print()
    
    # Plot of the file tasks per frame
    fig, ax = plt.subplots(1, figsize = [5,5])
    times_array = np.array(range(num_frames+padding_frames))*time_per_frame*1000
    ax.plot(times_array, tasks_at_frame, "*-")
    ax.set_title("tasks since each frame (%s)" % title)
    ax.set_xlabel("time (ms)")
    ax.set_ylabel("number of tasks")
    fname = "tasks_per_frame_(%s).png" % title
    plt.savefig(fname)
    # Offer the user the filename in case they wish to display it after
    print("saved task metadata file:", fname)
    
    
    # Plot of the task timings
    # currently sorted by task beginning time
    fig, ax = plt.subplots(1, figsize = [5,5])
    ax.plot(range(len(tasks_times)), tasks_times, "*")
    ax.set_title("Timing of Each Task (%s)" % title)
    ax.set_xlabel("task number (sorted by beginning time)")
    ax.set_ylabel("task duration (ms)")
    fname = "tasks_times_execution_order_(%s).png" % title
    plt.savefig(fname)
    # Offer the user the filename in case they wish to display it after
    print("saved task metadata file:", fname)
    
    # By core migrations
    fig, ax = plt.subplots(1, figsize = [5,5])
    ax.plot(core_hit_df["new_idx"], core_hit_tasks_times, "*", color='b', label="reused")
    ax.plot(migrated_df["new_idx"], migrated_tasks_times, "*", color='r', label="migrated")
    ax.plot(f_access_df["new_idx"], f_access_tasks_times, "*", color='g', label="first access")
    ax.plot([0, max(orderdf["new_idx"])], [core_hit_tt_mean, core_hit_tt_mean], "--", color='b', label="resued mean")
    ax.plot([0, max(orderdf["new_idx"])], [migrated_tt_mean, migrated_tt_mean], "--", color='r', label="migrated mean")
    ax.plot([0, max(orderdf["new_idx"])], [f_access_tt_mean, f_access_tt_mean], "--", color='g', label="first access mean")
    ax.set_title("Timing of Each Task (%s)" % title)
    ax.set_xlabel("task number (sorted by beginning time)")
    ax.set_ylabel("task duration (ms)")
    plt.legend()
    fname = "tasks_times_colored_by_migrations_(%s).png" % title
    plt.savefig(fname)
    # Offer the user the filename in case they wish to display it after
    print("saved task metadata file:", fname)
    
    # Do it again but sort them by execution duration
    tasks_times.sort() # Now sorted by task duration
    fig, ax = plt.subplots(1, figsize = [5,5])
    ax.plot(range(len(tasks_times)), tasks_times, "*")
    ax.set_title("Timing of Each Task (%s)" % title)
    ax.set_xlabel("task number (sorted by task duration)")
    ax.set_ylabel("task duration (ms)")
    fname = "tasks_times_sorted_(%s).png" % title
    plt.savefig(fname)
    # Offer the user the filename in case they wish to display it after
    print("saved task metadata file:", fname)
    
    
    
    frustration_arr = []
    for thing in pruned_orderdf["duration"]:
        frustration_arr.append(thing)
    # Now make a violin plot as well
    fig, ax = plt.subplots(1, figsize = [6,6])
    ax.violinplot(frustration_arr)
    ax.set_title("Timing of Each Task (%s)" % title)
    ax.set_xlabel("density")
    ax.set_ylabel("task duration (ms)")
    ax.set_xticks([])
    fname = "tasks_times_violin_(%s).png" % title
    plt.savefig(fname)
    # Offer the user the filename in case they wish to display it after
    print("saved task metadata file:", fname)
    
    
    ##### Make DF's again (sorted this time)
    pruned_orderdf = pruned_orderdf.sort_values("duration") # Now sorted by task duration
    pruned_orderdf["new_idx"] = np.arange(len(pruned_orderdf))
    pruned_migrated_df = pruned_orderdf[pruned_orderdf.loc[:,"migrated"] == "migrated"]
    pruned_core_hit_df = pruned_orderdf[pruned_orderdf.loc[:,"migrated"] == "reused"]
    pruned_f_access_df = pruned_orderdf[pruned_orderdf.loc[:,"migrated"] == "first access"]
    pruned_sim_L2_3hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_L2_hit"] == 3]
    pruned_sim_L2_2hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_L2_hit"] == 2]
    pruned_sim_L2_1hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_L2_hit"] == 1]
    pruned_sim_L2_miss_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_L2_hit"] == 0]
    pruned_sim_L3_3hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_L3_hit"] == 3]
    pruned_sim_L3_2hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_L3_hit"] == 2]
    pruned_sim_L3_1hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_L3_hit"] == 1]
    pruned_sim_L3_miss_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_L3_hit"] == 0]
    pruned_sim_MM_3hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_MM_hit"] == 3]
    pruned_sim_MM_2hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_MM_hit"] == 2]
    pruned_sim_MM_1hit_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_MM_hit"] == 1]
    pruned_sim_MM_miss_df = pruned_orderdf[pruned_orderdf.loc[:,"sim_MM_hit"] == 0]
    
    # Do it again but sort them by execution duration
    fig, ax = plt.subplots(1, figsize = [5,5])
    ax.plot(pruned_orderdf["new_idx"], pruned_orderdf["duration"], "*")
    ax.set_title("Timing of Each Task (%s)" % title)
    ax.set_xlabel("task number (sorted by task duration)")
    ax.set_ylabel("task duration (ms)")
    fname = "tasks_times_sorted_pruned_(%s).png" % title
    plt.savefig(fname)
    # Offer the user the filename in case they wish to display it after
    print("saved task metadata file:", fname)
    
    
    # and for the pruned data
    fig, ax = plt.subplots(1, figsize = [5,5])
    ax.plot(pruned_core_hit_df["new_idx"], pruned_core_hit_df["duration"], "*", color="b", label="reused")
    ax.plot(pruned_migrated_df["new_idx"], pruned_migrated_df["duration"], "*", color="r", label="migrated")
    ax.plot(pruned_f_access_df["new_idx"], pruned_f_access_df["duration"], "*", color="g", label="first access")
    ax.plot([0, max(orderdf["new_idx"])], [migrated_ptt_mean, migrated_ptt_mean], "--", color='r', label="reused mean")
    ax.plot([0, max(orderdf["new_idx"])], [core_hit_ptt_mean, core_hit_ptt_mean], "--", color='b', label="migrated mean")
    ax.plot([0, max(orderdf["new_idx"])], [f_access_ptt_mean, f_access_ptt_mean], "--", color='g', label="frst access mean")
    ax.set_title("Timing of Each Task (%s)" % title)
    ax.set_xlabel("task number (sorted by task duration)")
    ax.set_ylabel("task duration (ms)")
    plt.legend()
    fname = "tasks_times_sorted_pruned_core_migrations(%s).png" % title
    plt.savefig(fname)
    # Offer the user the filename in case they wish to display it after
    print("saved task metadata file:", fname)
    
    # and for the pruned data
    fig, ax = plt.subplots(1, figsize = [5,5])
    ax.plot(pruned_sim_L2_3hit_df["new_idx"], pruned_sim_L2_3hit_df["duration"], "*", color='b', label="sim_L2 3hit")
    ax.plot(pruned_sim_L2_2hit_df["new_idx"], pruned_sim_L2_2hit_df["duration"], "*", color='g', label="sim_L2 2hit")
    ax.plot(pruned_sim_L2_1hit_df["new_idx"], pruned_sim_L2_1hit_df["duration"], "*", color='y', label="sim_L2 1hit")
    ax.plot(pruned_sim_L2_miss_df["new_idx"], pruned_sim_L2_miss_df["duration"], "*", color='r', label="sim_L2 miss")
    ax.plot([0, max(orderdf["new_idx"])], [sL2_3hit_ptt_mean, sL2_3hit_ptt_mean], "--", color='b', label="sim 3hit mean")
    ax.plot([0, max(orderdf["new_idx"])], [sL2_2hit_ptt_mean, sL2_2hit_ptt_mean], "--", color='g', label="sim 2hit mean")
    ax.plot([0, max(orderdf["new_idx"])], [sL2_1hit_ptt_mean, sL2_1hit_ptt_mean], "--", color='y', label="sim 1hit mean")
    ax.plot([0, max(orderdf["new_idx"])], [sL2_miss_ptt_mean, sL2_miss_ptt_mean], "--", color='r', label="sim miss mean")
    ax.set_title("Timing of Each Task (%s)" % title)
    ax.set_xlabel("task number (sorted by task duration)")
    ax.set_ylabel("task duration (ms)")
    plt.legend()
    fname = "tasks_times_sorted_pruned_simulated_L2(%s).png" % title
    plt.savefig(fname)
    # Offer the user the filename in case they wish to display it after
    print("saved task metadata file:", fname)
    
    fig, ax = plt.subplots(1, figsize = [5,5])
    ax.plot(pruned_sim_L3_3hit_df["new_idx"], pruned_sim_L3_3hit_df["duration"], "*", color='b', label="sim_L3 3hit")
    ax.plot(pruned_sim_L3_2hit_df["new_idx"], pruned_sim_L3_2hit_df["duration"], "*", color='g', label="sim_L3 2hit")
    ax.plot(pruned_sim_L3_1hit_df["new_idx"], pruned_sim_L3_1hit_df["duration"], "*", color='y', label="sim_L3 1hit")
    ax.plot(pruned_sim_L3_miss_df["new_idx"], pruned_sim_L3_miss_df["duration"], "*", color='r', label="sim_L3 miss")
    ax.plot([0, max(orderdf["new_idx"])], [sL3_3hit_ptt_mean, sL3_3hit_ptt_mean], "--", color='b', label="sim 3hit mean")
    ax.plot([0, max(orderdf["new_idx"])], [sL3_2hit_ptt_mean, sL3_2hit_ptt_mean], "--", color='g', label="sim 2hit mean")
    ax.plot([0, max(orderdf["new_idx"])], [sL3_1hit_ptt_mean, sL3_1hit_ptt_mean], "--", color='y', label="sim 1hit mean")
    ax.plot([0, max(orderdf["new_idx"])], [sL3_miss_ptt_mean, sL3_miss_ptt_mean], "--", color='r', label="sim miss mean")
    ax.set_title("Timing of Each Task (%s)" % title)
    ax.set_xlabel("task number (sorted by task duration)")
    ax.set_ylabel("task duration (ms)")
    plt.legend()
    fname = "tasks_times_sorted_pruned_simulated_L3(%s).png" % title
    plt.savefig(fname)
    # Offer the user the filename in case they wish to display it after
    print("saved task metadata file:", fname)
    
    fig, ax = plt.subplots(1, figsize = [5,5])
    ax.plot(pruned_sim_MM_3hit_df["new_idx"], pruned_sim_MM_3hit_df["duration"], "*", color='b', label="sim_MM 3hit")
    ax.plot(pruned_sim_MM_2hit_df["new_idx"], pruned_sim_MM_2hit_df["duration"], "*", color='g', label="sim_MM 2hit")
    ax.plot(pruned_sim_MM_1hit_df["new_idx"], pruned_sim_MM_1hit_df["duration"], "*", color='y', label="sim_MM 1hit")
    ax.plot(pruned_sim_MM_miss_df["new_idx"], pruned_sim_MM_miss_df["duration"], "*", color='r', label="sim_MM miss")
    ax.plot([0, max(orderdf["new_idx"])], [sMM_3hit_ptt_mean, sMM_3hit_ptt_mean], "--", color='b', label="sim 3hit mean")
    ax.plot([0, max(orderdf["new_idx"])], [sMM_2hit_ptt_mean, sMM_2hit_ptt_mean], "--", color='g', label="sim 2hit mean")
    ax.plot([0, max(orderdf["new_idx"])], [sMM_1hit_ptt_mean, sMM_1hit_ptt_mean], "--", color='y', label="sim 1hit mean")
    ax.plot([0, max(orderdf["new_idx"])], [sMM_miss_ptt_mean, sMM_miss_ptt_mean], "--", color='r', label="sim miss mean")
    ax.set_title("Timing of Each Task (%s)" % title)
    ax.set_xlabel("task number (sorted by task duration)")
    ax.set_ylabel("task duration (ms)")
    plt.legend()
    fname = "tasks_times_sorted_pruned_simulated_MM(%s).png" % title
    plt.savefig(fname)
    # Offer the user the filename in case they wish to display it after
    print("saved task metadata file:", fname)
    
    
    if(sum(tasks_at_frame) != expected_tasks):
        print("error: expected %d tasks but I only observed %d in the trace" %
              (expected_tasks, sum(tasks_at_frame)))
    
    # print(f"len(f_access_df):{(len(f_access_df))}\t expected first accesses: {Ntiles_n * Ntiles_m}")
    if(len(f_access_df) != Ntiles_n * Ntiles_m):
        print("error: something isn't right about the first tasks policy")
        print(f"    I counted {len(f_access_df)} first accesses but expected {Ntiles_n * Ntiles_m}!")
    
    
    print()