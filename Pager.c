#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>

typedef struct {
  int file_descriptor;
  uint32_t file_length;
  void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
  Pager* pager;
   uint32_t num_rows;
 } Table;

//change (rename) new_table fxn to open_database
// create Pager* pager, initialize table and pager values 
// accordingly
//for fd
int fd = open(filename,O_RDWR |      // Read/Write mode
                    O_CREAT,  // Create file if it does not exist
                S_IWUSR |     // User write permission
                    S_IRUSR   // User read permission
                );

  if (fd == -1) {
    printf("Unable to open file\n");
    exit(EXIT_FAILURE);
  }


// in row_slot fxn, get page from pager
// handle partial page/ new page case
//Ex: 
if (pager->pages[page_num] == NULL) 
    // Cache miss. Allocate memory and load from file.



//  flush the cache to disk only when .exit is done. Call db_close() upon exit:
// db_close() does the following:
//     flushes the page cache to disk
//     closes the database file
//     frees the memory for the Pager and Table data structures
// Go through each page of pager and do the above things
// handle partial page also

off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
write(pager->file_descriptor, pager->pages[page_num], size);

int main(int argc, char* argv[]){
    char* filename = argv[1];
    ...
}


//Extra: memcpy can have certain issues, what are they, what can be used instead