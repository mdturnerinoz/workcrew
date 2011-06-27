# Make file for the IBM pthreads workcrew tutorial example code
CC      := gcc
CFLAGS  := -c -g -O0
EXE     := workcrew
SRC     := queue.c workcrew.c control.c
OBJ     := $(subst .c,.o,$(SRC))
$(EXE)  : $(OBJ)	
	gcc $(OBJ) -o $(EXE) -lpthread
clean :
	rm -f *~ *.o $(EXE) typescript
