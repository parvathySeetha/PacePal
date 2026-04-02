import requests
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import text_from_rendered
import os

def convert_pdf_to_markdown(pdf_path: str, output_path: str = None) -> str:
    """
    Converts a PDF file to Markdown using the marker library.
    
    Args:
        pdf_path: Path to the input PDF file.
        output_path: Optional path to save the output Markdown file.
        
    Returns:
        The generated Markdown content as a string.
    """
    print(f"Converting PDF to Markdown: {pdf_path}")
    try:
        converter = PdfConverter(
            artifact_dict=create_model_dict(),
        )
        rendered = converter(pdf_path)
        text, _, images = text_from_rendered(rendered)

        if output_path:
            print(f"Saving output to {output_path}...")
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(text)

        print("Conversion Done.")
        return text
    except Exception as e:
        print(f"Error converting PDF: {e}")
        raise e

if __name__ == "__main__":
    # Example usage
    filename = "downloads/2211119556_Sales Order_TMH___iPhone_14_Nov_Dec_2022_FB_Twitter_Tiktok_Online Media.pdf"
    convert_pdf_to_markdown(filename, "output10.md")
