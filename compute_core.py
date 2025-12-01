import torch
import platform
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_device() -> torch.device:
    """
    Detects and returns the best available compute device.
    Priority: CUDA (Nvidia) > ROCm (AMD) > MPS (Apple Silicon) > CPU.
    """
    if torch.cuda.is_available():
        # Check for AMD ROCm specifically if needed, though torch.cuda usually handles it if installed correctly
        if torch.version.hip:
            logger.info("AMD ROCm detected.")
            return torch.device("cuda")
        
        logger.info("Nvidia CUDA detected.")
        return torch.device("cuda")
    
    if torch.backends.mps.is_available():
        logger.info("Apple MPS (Metal Performance Shaders) detected.")
        return torch.device("mps")
    
    logger.info("No GPU detected. Falling back to CPU.")
    return torch.device("cpu")

def device_info() -> str:
    """
    Returns a human-readable string describing the active hardware backend.
    """
    device = get_device()
    
    if device.type == "cuda":
        device_name = torch.cuda.get_device_name(0)
        if torch.version.hip:
            return f"Running on AMD GPU ({device_name}) via ROCm/HIP"
        return f"Running on Nvidia GPU ({device_name}) via CUDA"
    
    if device.type == "mps":
        return f"Running on Apple Silicon via MPS"
    
    # CPU optimizations check
    info = f"Running on CPU ({platform.processor()})"
    if torch.backends.mkldnn.is_available():
        info += " with MKL-DNN optimizations"
    
    return info

def configure_cpu_optimizations():
    """
    Configures PyTorch for optimal CPU performance if no GPU is available.
    """
    if get_device().type == "cpu":
        # Enable MKL-DNN if available
        if torch.backends.mkldnn.is_available():
            torch.backends.mkldnn.enabled = True
            logger.info("MKL-DNN enabled for CPU.")
        
        # Set number of threads to avoid oversubscription if needed
        # torch.set_num_threads(os.cpu_count()) # Optional, usually torch handles this well

if __name__ == "__main__":
    print(device_info())
