# Daniel Mishler
# Last push to github 2022-06-08

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from IPython import display
import numpy as np
import math
import time


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

    
# Some globals are needed for the below function:
# A_status, B_status, C_status, A_expected, B_expected, C_expected
# These lists of lists represent the progress of the job on each tile
# Such lists are modified both by animate_trace and a function within it,
# which is necessary to implement in order to achieve the format that 
# is required by FuncAnimation.
# The `expected` variables are so that calculating progress as a percentage
# is feasible.
# The last globals are
# `tasks_at_frame`
# This is an array of the number of tasks which have executed since the previous
# frame.

#### `orderdf`
#### a dataframe containing the order the trace runs in. Set up during the init function.
def animate_trace(trace, 
                  trace_type,
                  task_type,
                  order_func = None,
                  which_animate = "tasks",
                  title = "unknown desc (pass something as 'title')",
                  num_frames = 50,         # Number of frames to use in the animation
                  enforce_interval = None, # Alternatively, determines a timestep per frame (in seconds)
                  fps = 13,
                  M=4000,
                  N=4000,
                  K=4000,
                  tilesize = 200):
    
    global A_status
    global B_status
    global C_status
    global tasks_at_frame
    global orderdf
    global A_expected # Currently unimplemented
    global B_expected
    global C_expected
    start_time = time.time()
    print("Beginning animation of data '%s' method '%s'" % (title, which_animate))
    
    # Begin checks
    if trace_type == "ptg":
        pass
    elif trace_type == "dtd":
        if order_func is None:
            print("Error: for DTD traces, you must supply an order function!")
            return
    else:
        print("Error: unknown trace type '" + str(trace_type) + "'")
        return
    
    
    if task_type == "gemm":
        pass
    elif task_type == "potrf":
        if trace_type == "ptg":
            pass
        else:
            print("Error: potrf only supported with ptg")
            return
        if(which_animate == "abcprogress" or which_animate == "abctasks"):
            print("Error: potrf only supported with C view")
            return
        else:
            pass
    else:
        print("Error: unknown task type '" + str(task_type) +"'")
        return
    
    if which_animate not in ["tasks", "abctasks", "progress", "abcprogress"]:
        print("warning, I don't know what you wanted me to animate (which_animate='%s')" % which_animate)
        print("I'll animate the progress animation")
        which_animate = "progress"
    # Begin estimation and preprocessing
    try:
        trace.information["HWLOC-XML"]
        running_system = "dplasma"
    except KeyError:
        running_system = "hicma"
    print("I think this trace is for a task that was running on", running_system)
    work_tasks_indices = []
    if task_type == "gemm":
        gemm_index_found = 0
        for i in trace.event_types.index: # An array of numbers indexed by their name. Interesting, huh?
            if i.lower() == "gemm":
                work_tasks_indices.append(trace.event_types[i])
                gemm_index_found += 1
                break
        if gemm_index_found != 1:
            print("Error: file trace does not have its event_types set properly")
            print("found %d events, expected %d" % (gemm_index_found, 1))
            return
    
    elif task_type == "potrf":
        name_to_task_num = {}
        potrf_index_found = 0
        for i in trace.event_types.index:
            if "potrf_" in i.lower(): # Could possibly use regex for this...
                print("found task type", i.lower())
                work_tasks_indices.append(trace.event_types[i])
                for taskname in ["gemm", "syrk", "trsm", "potrf"]:
                    if(taskname in i.lower()):
                        name_to_task_num[taskname] = trace.event_types[i]
                        potrf_index_found += 1
                        break
                # TODO: get rid of this thing for tasks with additional potrfs
                if potrf_index_found == 4:
                    break
        if potrf_index_found != 4:
            print("Error: file trace does not have its event_types set properly")
            print("found %d events, expected %d" % (potrf_index_found, 4))
            return
    
    # Start with an all false indexing structure.
    # TODO: learn more about what this thing is.
    work_tasks = (trace.events.type == 0) & (trace.events.type == 1)
    
    # build work_tasks while making sure that for each individual task type,
    # the indices are unique
    for i in work_tasks_indices:
        work_task = trace.events.type == i
        work_tasks |= work_task
        if (len(trace.events[work_task]["id"].unique()) != len(trace.events[work_task]["id"])):
            print("Warning: file does not have fully unique tasks for task type %d" % i)
        
    orderdf = pd.DataFrame(trace.events[work_tasks].sort_values("begin"))
        
        
    first_begin = orderdf["begin"].iloc[0]
    last_end =  orderdf["end"].iloc[-1]
    
    Ntiles_m = math.ceil(M/tilesize)
    Ntiles_n = math.ceil(N/tilesize)
    Ntiles_k = math.ceil(K/tilesize)
    
    # Possibly enforce an interval in seconds
    if enforce_interval is not None:
        time_per_frame = enforce_interval
        num_frames = math.ceil((last_end - first_begin)/10**9/time_per_frame)
        print("enforcing %d frames to grant you your requested enforced interval" % (num_frames))
    time_per_frame = (last_end - first_begin)/10**9/num_frames
    print("process runtime per frame: %f seconds" % (time_per_frame))
    
    if(which_animate == "abcprogress" or which_animate == "abctasks"):
        estimation_multiplier = 3 # for abc type calls
    else:
        estimation_multiplier = 1
    print("estimated execution time (assuming a lightweight commercial processor): %f seconds" %
          (num_frames * 0.0004 * Ntiles_m * Ntiles_n * estimation_multiplier))
    if(num_frames > 100):
        print("animate_trace warning: num_frames beyond 100 may cause long compute time")
    if(num_frames > 500):
        print("animate_trace warning: num_frames beyond 500 may cause extreme compute time")
    if(num_frames <= 0):
        print("Error: illegal number of frames. Must be at least 1")
        return
    
    # End estimation and preprocessing
    if task_type == "gemm":
        expected_tasks = Ntiles_m * Ntiles_n * Ntiles_k
    elif task_type == "potrf":
        if Ntiles_m != Ntiles_n:
            print("error: expected 'n' and 'm' to be equal to POTRF trace")
            return
        expected_tasks = Ntiles_m # potrf
        expected_tasks += ((Ntiles_m-1)*(Ntiles_m)) // 2 # trsm
        expected_tasks += ((Ntiles_m-1)*(Ntiles_m)) // 2 # syrk
        for i in range(Ntiles_m-1): # gemm 
            expected_tasks += (i*(i+1)) // 2
        #TODO: use the sum of squares of natural numbers formula
        
        # Alternative methods
        # n * 1 + (n-1) * 2 + (n-2) * 3 + (n-3) * 4 + ...
        # which is same as: sum over (x^2/2 + x/2) from 1 to n
        # which is the same as: (n) * (n+1) * (n+2) / 6
    
    
    # This ID array is only needed for dtd traces.
    # (Because the trace for DTD gemm does not supply us with
    # m, n, and k, we must remember the order we insterted the tasks
    # and re-extract that data as such:
    if trace_type == "dtd":
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
    
    
    C_status = []
    C_expected = []
    for i in range(Ntiles_m):
        C_status.append(np.zeros(Ntiles_n))
        C_expected.append(np.zeros(Ntiles_n))
    C_status = np.array(C_status)
    C_expected = np.array(C_expected)
    
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
    
    
    # Prepare the figure that will be displayed
    fld = 6.5 # figsize larger dimension
    fmd = 3.5 # figsize minimum dimension
    if Ntiles_m >= Ntiles_n:
        figsize_x = fld*Ntiles_n/Ntiles_m
        figsize_y = fld
        if(figsize_x < fmd):
            figsize_x = fmd
    else:
        figsize_x = fld
        figsize_y = fld*Ntiles_m/Ntiles_n
        if(figsize_y < fmd):
            figsize_y = fmd
            
    visual_vmax = Ntiles_k # TODO: remove if percentage is the way to go
    # Make the figure
    if(which_animate == "abcprogress" or which_animate == "abctasks"):
        fig, ((axx, axB), (axA, axC)) = plt.subplots(2,2, figsize = [figsize_x, figsize_y])
        # fig.colorbar(axC.pcolormesh(C_status,vmin=0,vmax=visual_vmax))
        axA.pcolormesh(A_status, vmin = 0, vmax = 1)
        axA.invert_yaxis()
        axB.pcolormesh(B_status, vmin = 0, vmax = 1)
        axB.invert_yaxis()
        axC.pcolormesh(C_status, vmin = 0, vmax = 1)
        axC.invert_yaxis()
    else:
        fig, ax = plt.subplots(1, figsize = [figsize_x, figsize_y])
        ax.pcolormesh(C_status, vmin = 0, vmax = visual_vmax)
        fig.colorbar(ax.pcolormesh(C_status,vmin=0,vmax=1))
        ax.invert_yaxis()
    
    # Enter the animation functions
    # plots: 'c' for just plotting matrix c, 'abc' for all three
    # mode: 'progress' for keeping the tiles colored, 'tasks' to reset them and see what was active and when.
    # FuncAnimate works best with an init function, even if it doesn't do much or the init is done elsewhere.
    def animate_init_common(plots):
        global A_status
        global B_status
        global C_status
        global tasks_at_frame
        global orderdf
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
            
        if task_type == "gemm":
            C_expected += Ntiles_k
            if plots == "abc":
                A_expected += Ntiles_n
                B_expected += Ntiles_m
        elif task_type == "potrf": # TODO: add tasks to this animation function, have it be an even C_expected
            for i in range(len(C_expected)):
                for j in range(len(C_expected[0])):
                    if j <= i:
                        C_expected[i,j] = j+1
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
        global tasks_at_frame
        global orderdf
        global A_expected
        global B_expected
        global C_expected
        # time_point_curr and time_point_prev in units of nanoseconds
        time_point_curr = ((frame+1)*(last_end - first_begin))//num_frames + first_begin
        time_point_prev = ((frame+0)*(last_end - first_begin))//num_frames + first_begin
        if(frame >= num_frames): # This construction to allow dead frames at the end
            time_point_prev = time_point_curr = last_end
            
        if(mode == "tasks"):
            C_status = C_status * 0 # Always zero when doing task timing
            if(plots == "abc"):
                A_status = A_status * 0 # Always zero when doing task timing
                B_status = B_status * 0 # Always zero when doing task timing 
                
        tasks_before = orderdf.loc[orderdf["end"] <= time_point_curr]
        tasks_during = tasks_before.loc[(tasks_before["end"] > time_point_prev)]
            # aside: you might think this construction with > time_point_prev wouldn't include the first task,
            #        but this is fine! The only assumption made is that the first tasks does not end the same nanoseond
            #        that it begins. That sounds like a safe assumption to me.
        
        tasks_at_frame.append(len(tasks_during))
        
        if trace_type == "dtd":
            for tid in tasks_during["id"]:
                tid_normed = indices_arr[np.where(id_orders == tid)][0]
                element = ideal_order[indices_arr[tid_normed]]
                m = element[0]
                n = element[1]
                k = element[2]
                if(plots == "c"):
                    C_status[m, n] += 1 / C_expected[m, n]
                elif(plots == "abc"):
                    A_status[m, k] += 1 / A_expected[m, k]
                    B_status[k, n] += 1 / B_expected[k, n]
                    C_status[m, n] += 1 / C_expected[m, n]
        elif trace_type == "ptg":
            for index, task in tasks_during.iterrows():
                m = task["m"]
                n = task["n"]
                k = task["k"]
                if(plots == "c"):
                    C_status[m, n] += 1 / C_expected[m, n]
                elif(plots == "abc"):
                    A_status[m, k] += 1 / A_expected[m, k]
                    B_status[k, n] += 1 / B_expected[k, n]
                    C_status[m, n] += 1 / C_expected[m, n]
                
        vmax_A = Ntiles_n
        vmax_B = Ntiles_m
        vmax_C = Ntiles_k # TODO: remove these if we go through with the percentage calculation
        if(plots == "c"):
            ax.set_title("Matrix C at time t=%fs (%s)" % (time_point_curr/10**9, title))
            ax.pcolormesh(C_status, vmin = 0, vmax = 1)
        elif(plots == "abc"):
            axA.set_title("Matrix A")
            axA.pcolormesh(A_status, vmin = 0, vmax = 1)
            axB.set_title("Matrix B")
            axB.pcolormesh(B_status, vmin = 0, vmax = 1)
            axC.set_title("Matrix C")
            axC.pcolormesh(C_status, vmin = 0, vmax = 1)
        return
    
    def animate_gemm_dtd_order_with_time(frame):
        return animate_gemm_common(frame, "dtd", "c", "tasks")
    
    def animate_gemm_dtd_order_with_time_all(frame):
        return animate_gemm_common(frame, "dtd", "abc", "tasks")
    
    def animate_gemm_dtd_order_progress(frame):
        return animate_gemm_common(frame, "dtd", "c", "progress")
        
    def animate_gemm_dtd_order_progress_all(frame):
        return animate_gemm_common(frame, "dtd", "abc", "progress")
    
    def animate_gemm_ptg_order_with_time(frame):
        return animate_gemm_common(frame, "ptg", "c", "tasks")
    
    def animate_gemm_ptg_order_with_time_all(frame):
        return animate_gemm_common(frame, "ptg", "abc", "tasks")
    
    def animate_gemm_ptg_order_progress(frame):
        return animate_gemm_common(frame, "ptg", "c", "progress")
        
    def animate_gemm_ptg_order_progress_all(frame):
        return animate_gemm_common(frame, "ptg", "abc", "progress")
    
    def animate_potrf_common(frame, mode):
        global C_status
        global tasks_at_frame
        global orderdf
        global C_expected
        # time_point_curr and time_point_prev in units of nanoseconds
        time_point_curr = ((frame+1)*(last_end - first_begin))//num_frames + first_begin
        time_point_prev = ((frame+0)*(last_end - first_begin))//num_frames + first_begin
        if(frame >= num_frames): # This construction to allow dead frames at the end
            time_point_prev = time_point_curr = last_end
            
        if(mode == "tasks"):
            C_status = C_status * 0 # Always zero when doing task timing
            
        tasks_before = orderdf.loc[orderdf["end"] <= time_point_curr]
        tasks_during = tasks_before.loc[(tasks_before["end"] > time_point_prev)]
            # aside: you might think this construction with > time_point_prev wouldn't include the first task,
            #        but this is fine! The only assumption made is that the first tasks does not end the same nanoseond
            #        that it begins. That sounds like a safe assumption to me.
        
        tasks_at_frame.append(len(tasks_during))
        
        for index, task in tasks_during.iterrows():
            m = task["m"]
            n = task["n"]
            k = task["k"]
            # print("type:", task["type"], "m,n,k:", m,n,k)
            # dplasma and hicma order the m, n, and k differently
            if running_system == "dplasma":
                if task["type"] == name_to_task_num["potrf"]:
                    if k is None:
                        print("k should not be none here")
                    C_status[k, k] += 1 / C_expected[k, k]
                elif task["type"] == name_to_task_num["trsm"]:
                    if k is None:
                        print("k should not be none here")
                    if n is None:
                        print("n should not be none here")
                    C_status[n, k] += 1 / C_expected[n, k]
                elif task["type"] == name_to_task_num["syrk"]:
                    if n is None:
                        print("n should not be none here")
                    C_status[n, n] += 1 / C_expected[n, n]
                elif task["type"] == name_to_task_num["gemm"]:
                    if m is None:
                        print("m should not be none here")
                    if n is None:
                        print("n should not be none here")
                    C_status[n, m] += 1 / C_expected[n, m]
                else:
                    print(task)
                    print("error: unexpected task of type %d" % task["type"])
            elif running_system == "hicma":
                if task["type"] == name_to_task_num["potrf"]:
                    if k is None:
                        print("k should not be none here (potrf)")
                    C_status[k, k] += 1 / C_expected[k, k]
                elif task["type"] == name_to_task_num["trsm"]:
                    if m is None:
                        print("m should not be none here (trsm)")
                    if k is None:
                        print("k should not be none here (trsm)")
                    C_status[m, k] += 1 / C_expected[m, k]
                elif task["type"] == name_to_task_num["syrk"]:
                    if m is None:
                        print("m should not be none here (syrk)")
                    C_status[m, m] += 1 / C_expected[m, m]
                elif task["type"] == name_to_task_num["gemm"]:
                    if m is None:
                        print("m should not be none here (gemm)")
                    if n is None:
                        print("n should not be none here (gemm)")
                    C_status[m, n] += 1 / C_expected[m, n]
                else:
                    print(task)
                    print("error: unexpected task of type %d" % task["type"])
            else:
                print("Error: unknown running system")
                
        vmax_A = Ntiles_n
        vmax_B = Ntiles_m
        vmax_C = Ntiles_k
        
        ax.set_title("Matrix C at time t=%fs (%s)" % (time_point_curr/10**9, title))
        ax.pcolormesh(C_status, vmin = 0, vmax = 1)
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
            elif(which_animate == "abctasks"):
                animation_func = animate_gemm_dtd_order_with_time_all
                animation_init = animate_init_abc
            elif(which_animate == "abcprogress"):
                animation_func = animate_gemm_dtd_order_progress_all
                animation_init = animate_init_abc
        elif(trace_type == "ptg"):
            if(which_animate == "tasks"):
                animation_func = animate_gemm_ptg_order_with_time
                animation_init = animate_init_c
            elif(which_animate == "progress"):
                animation_func = animate_gemm_ptg_order_progress
                animation_init = animate_init_c
            elif(which_animate == "abctasks"):
                animation_func = animate_gemm_ptg_order_with_time_all
                animation_init = animate_init_abc
            elif(which_animate == "abcprogress"):
                animation_func = animate_gemm_ptg_order_progress_all
                animation_init = animate_init_abc
    elif (task_type == "potrf"):
        animation_init = animate_init_c
        if(which_animate == "tasks"):
            animation_func = animate_potrf_tasks
        elif(which_animate == "progress"):
            animation_func = animate_potrf_progress
        
    mid_time = time.time()
    padding_frames = fps // 2 # Supply a half second of stillness at the end
    animation_result = FuncAnimation(fig,animation_func, init_func = animation_init,
                                     frames=(num_frames+padding_frames),interval=1000//fps)
    video = animation_result.to_html5_video()
    html = display.HTML(video)
    display.display(html)
    plt.close()
    

    end_time = time.time()
    ## Printing
    tasks_times = (orderdf["end"] - orderdf["begin"]).to_numpy()
    tasks_times = tasks_times / 1000000 # convert from ns to ms
    tasks_execution_mean = tasks_times.mean()
    tasks_execution_sdev = tasks_times.std()
    tasks_runtime = (last_end - first_begin) / 1000000 # in ms
    utilization = (tasks_execution_mean * len(tasks_times)) / tasks_runtime
    num_cores = trace.information["nb_cores"]
    print("Data titled '%s'" % title)
    print("M=%d,\tN=%d,\tK=%d,\ttilesize=%d" % (M,N,K,tilesize))
    print("average task execution time: %f" % tasks_execution_mean)
    print("task execution time standard deviation: %f" % tasks_execution_sdev)
    print("utilization: %f over %d cores (%f)" % (utilization, num_cores, utilization/num_cores))
    print("execution time to generate graphs: %f seconds" % (end_time-mid_time))
    print("execution time to preprocess data: %f seconds" % (mid_time-start_time))
    print("execution time total: %f seconds" % (end_time-start_time))
    
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
    
    if(sum(tasks_at_frame) != expected_tasks):
        print("error: expected %d tasks but I only observed %d in the trace" %
              (expected_tasks, sum(tasks_at_frame)))
    
    """
    if(sum(sum(C_status)) != expected_tasks):
        print("error: C_status expected updates %d times but received %d updates" %
              (expected_tasks, sum(sum(C_status))))
    """
    
    print()
