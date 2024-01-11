# shapes_cython.pyx

# Import necessary Cython declarations
from libc.stdlib cimport malloc, free
from cpython.dict cimport PyDict_New
from cpython.set cimport PySet_New
from libcpp.vector cimport vector

# For Line._init_cys()
cpdef _init_cys(int n, tuple xc_symbol_pairs, dict cys_map, int start_direction):
    # set start_direction to -1 to mean `None`
    cdef:
        int i, j
        list split = []
        list cys = []
        list cy_sides = []
        tuple prev, cur, nxt
        str cy, _cy
        int cy_side
    split = [
        tuple(s.split("/")) if (xc, s) not in cys_map else cys_map[(xc, s)]
        for xc, s in xc_symbol_pairs
    ]
    #if start_direction is not None:
    #    start_direction = as_direction(start_direction)
    cy = ""

    for i in range(0, n):
        prev, cur, nxt = split[(i - 1) % n], split[i], split[(i + 1) % n]
        if not i and start_direction > 0:
            cy, cy_side = split[i][start_direction], int(not start_direction)
        elif not i:
            for j, _cy in enumerate(cur[::-1]):
                if _cy not in nxt:
                    cy, cy_side = _cy, j
                    break
            if cy == "":
                cy, cy_side = split[0][1], 0
        else:
            for j, _cy in enumerate(cur[::-1]):
                if _cy in prev and _cy != cys[i - 1]:
                    cy, cy_side = _cy, j 
                    break
            if cy == "":
                raise ValueError(
                    "Symbol path isn't linear at [{}:{}]: {}".format(
                        i - 1, i, xc_symbol_pairs
                    )
                )
        cys.append(cy)
        cy_sides.append(cy_side)
        cy = ""

    final_cy_side = int(not (cy_sides[-1]))
    final_cy = split[-1][not final_cy_side]

    cys += [final_cy]
    cy_sides += [final_cy_side]

    return cys, cy_sides


# Define the function with types
cdef _rec_cy_trail(tuple trail, int max_n, tuple n_values, dict currency_graph, dict n_shapes_of_currencies, set _seen_currency_shapes):
    cdef:
        str last_cy = trail[-1]
        int n = len(trail)
        int is_length_included = n in n_values
        int is_length_unsaturated = n < max_n
        str next_cy
        int is_circular
        list trail_list, trail_forwards, trail_backwards
        int alphabetical_loc
        tuple forwards_tuple, backwards_tuple

    for next_cy in currency_graph[last_cy]:
        if next_cy in trail[1:]:
            continue

        is_circular = next_cy == trail[0]

        if is_length_included and is_circular:
            # Convert tuple to list for manipulation
            trail_list = list(trail)
            alphabetical_loc = trail_list.index(sorted(trail_list)[0])

            trail_forwards = trail_list[alphabetical_loc:] + trail_list[:alphabetical_loc]
            trail_backwards = [trail_forwards[0]] + trail_forwards[-1:0:-1]

            # Convert lists back to tuples for set operations
            forwards_tuple = tuple(trail_forwards)
            backwards_tuple = tuple(trail_backwards)

            if (forwards_tuple not in _seen_currency_shapes and
                backwards_tuple not in _seen_currency_shapes):
                _seen_currency_shapes.add(forwards_tuple)
                _seen_currency_shapes.add(backwards_tuple)
                n_shapes_of_currencies[n].add(forwards_tuple)

        if is_length_unsaturated and not is_circular:
            _rec_cy_trail(trail + (next_cy,), max_n, n_values, currency_graph, n_shapes_of_currencies, _seen_currency_shapes)


cpdef create_shapes_of_currencies(tuple n_values, dict currency_graph):
    cdef:
        set _seen_currency_shapes = set()
        dict n_shapes_of_currencies = {i: set() for i in n_values}  # Adjust this if you have specific types for n_values
        int max_n = max(n_values)

    for cy in currency_graph:
        _rec_cy_trail((cy,), max_n, n_values, currency_graph, n_shapes_of_currencies, _seen_currency_shapes)
    
    return n_shapes_of_currencies
