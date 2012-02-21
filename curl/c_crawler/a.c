int* stack;
int StackInit(int i)
{
  stack = malloc(i*(sizeof(int));
  return stack;
}

void push(int elem)
{
  printf("length %f", (sizeof(stack)/sizeof(int)));
  int top = (sizeof(stack)/sizeof(int));
  stack[++top] = elem;
}
int pop(int elem)
{
  return stack[elem--];
}

int main()
{
  int j = 10;
   
  int l;
  /* allocating a stack */
  stackinit(j);
  /* fill the stack with 10 items */
  for(l=0;l<=10;l++){
    push(l);
  
   /* we have 10 items in the stack, so quit */
    if(l >= 10)
       {
        
         break;
         exit(1);
       }
   }
  /* pop out all items of the stack */
   int k;
   for(k = 10;k>=0;k--)
    {
      pop(k);
      if(k <= 0)
        {
          break;
          exit(1);
        }
    }
 return 0;
}
