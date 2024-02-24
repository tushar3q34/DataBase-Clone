#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>


//cursor abstraction


    // Create a cursor at the beginning of the table
    // Create a cursor at the end of the table
    // Access the row the cursor is pointing to
    // Advance the cursor to the next row

// Use (in further tasks)


    // Delete the row pointed to by a cursor
    // Modify the row pointed to by a cursor
    // Search a table for a given ID, and create a cursor pointing to the row with that ID

   typedef struct {
  Table* table;
  uint32_t row_num;
  bool end_of_table;  // Indicates a position one past the last element
} Cursor;

Cursor* table_start(){...} 
Cursor* table_end() {...}
//creates new cursors

// Now, row_slot() fxn renamed to cursor_location()
// modify implementation accordingly
void* cursor_location(Cursor* cursor){...}

// next row
void cursor_advance(Cursor* cursor){...}

// use the above cursor fxns (instead of rows) in
// insert (table_end, etc.)
// select (table_begin, aursor_advance, etc.)