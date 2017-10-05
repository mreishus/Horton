namespace Horton
{
    public abstract class HortonCommand
    {
        public abstract void ExecuteAsync(HortonOptions options);
    }
}